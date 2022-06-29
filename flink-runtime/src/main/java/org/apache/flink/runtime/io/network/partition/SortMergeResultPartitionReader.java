/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Data reader for {@link SortMergeResultPartition} which can read data for all downstream tasks
 * consuming the corresponding {@link SortMergeResultPartition}. It always tries to read shuffle
 * data in order of file offset, which maximums the sequential read so can improve the blocking
 * shuffle performance.
 */
class SortMergeResultPartitionReader implements Runnable, BufferRecycler {

    private static final Logger LOG = LoggerFactory.getLogger(SortMergeResultPartitionReader.class);

    private static final long MIN_READING_BUFFER_SIZE = (long) 1024 * 1024;

    /**
     * Default maximum time (5min) to wait when requesting read buffers from the buffer pool before
     * throwing an exception.
     */
    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    /** Lock used to synchronize multi-thread access to thread-unsafe fields. */
    private final Object lock;

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /**
     * A {@link CompletableFuture} to be completed when this read scheduler including all resources
     * is released.
     */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Buffer pool from which to allocate buffers for shuffle data reading. */
    private final BatchShuffleReadBufferPool bufferPool;

    /** Executor to run the shuffle data reading task. */
    private final Executor ioExecutor;

    /** Maximum number of buffers can be allocated by this partition reader. */
    private final int maxRequestedBuffers;

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    /** All failed subpartition readers to be released. */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionView> failedViews = new HashSet<>();

    /** All readers waiting to read data of different subpartitions. */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionView> allViews = new HashSet<>();

    /** File channel shared by all subpartitions to read data from. */
    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    /** File channel shared by all subpartitions to read index from. */
    @GuardedBy("lock")
    private FileChannel indexFileChannel;

    /**
     * Whether the data reading task is currently running or not. This flag is used when trying to
     * submit the data reading task.
     */
    @GuardedBy("lock")
    private boolean isRunning;

    /** Number of buffers already allocated and still not recycled by this partition reader. */
    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    /** Whether this reader has been released or not. */
    @GuardedBy("lock")
    private volatile boolean isReleased;

    SortMergeResultPartitionReader(
            int numSubpartitions,
            BatchShuffleReadBufferPool bufferPool,
            Executor ioExecutor,
            Object lock) {
        this(numSubpartitions, bufferPool, ioExecutor, lock, DEFAULT_BUFFER_REQUEST_TIMEOUT);
    }

    SortMergeResultPartitionReader(
            int numSubpartitions,
            BatchShuffleReadBufferPool bufferPool,
            Executor ioExecutor,
            Object lock,
            Duration bufferRequestTimeout) {
        checkArgument(bufferPool.getBufferSize() > HEADER_LENGTH, "Too small buffer size.");

        this.lock = checkNotNull(lock);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        // one partition reader can consume at most Math.max(16M, numSubpartitions) (the expected
        // buffers per request is 8M) buffers for data read, which means larger parallelism, more
        // buffers. Currently, it is only an empirical strategy which can not be configured.
        this.maxRequestedBuffers =
                Math.max(2 * bufferPool.getNumBuffersPerRequest(), numSubpartitions);
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);

        this.indexEntryBuf = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBuf);
    }

    @Override
    public synchronized void run() {
        List<ReadingRequest> readingRequests = computeReadingRequests();
        if (readingRequests.isEmpty()) {
            return;
        }

        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Throwable throwable) {
            // fail all pending subpartition readers immediately if any exception occurs
            failSubpartitionReaders(getAllSubpartitionViews(), throwable);
            LOG.error("Failed to request buffers for data reading.", throwable);
            return;
        }
        int numBuffersAllocated = buffers.size();

        Set<SortMergeSubpartitionView> finishedViews = readData(readingRequests, buffers);

        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);

        removeFinishedAndFailedViews(numBuffersRead, finishedViews);
    }

    @VisibleForTesting
    Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            checkState(!isReleased, "Result partition has been already released.");
        } while (System.nanoTime() < timeoutTime
                || System.nanoTime() < (timeoutTime = getBufferRequestTimeoutTime()));

        if (numRequestedBuffers <= 0) {
            throw new TimeoutException(
                    String.format(
                            "Buffer request timeout, this means there is a fierce contention of"
                                    + " the batch shuffle read memory, please increase '%s'.",
                            TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
        }
        return new ArrayDeque<>();
    }

    private ArrayList<SortMergeSubpartitionView> getAllSubpartitionViews() {
        synchronized (lock) {
            return new ArrayList<>(allViews);
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toNanos();
    }

    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    private Set<SortMergeSubpartitionView> readData(
            List<ReadingRequest> readingRequests, Queue<MemorySegment> buffers) {
        Set<SortMergeSubpartitionView> finishedViews = new HashSet<>();
        BufferReaderWriterUtil.BufferHeader currentHeader = null;
        ByteBuffer headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

        for (ReadingRequest readingRequest : readingRequests) {
            if ((long) buffers.size() * bufferPool.getBufferSize() < MIN_READING_BUFFER_SIZE) {
                break;
            }

            long requestedBytes = readingRequest.endOffset - readingRequest.startOffset;
            int bufferSize = bufferPool.getBufferSize();
            long maxRequiredBuffers =
                    requestedBytes / bufferSize + (requestedBytes % bufferSize == 0 ? 0 : 1);
            int numBuffers = Math.max((int) Math.min(maxRequiredBuffers, buffers.size()), 1);
            ByteBuffer[] readingBuffers = new ByteBuffer[numBuffers];
            List<MemorySegment> readingSegments = new ArrayList<>(numBuffers);
            for (int i = 0; i < readingBuffers.length; ++i) {
                MemorySegment segment = checkNotNull(buffers.poll());
                readingSegments.add(segment);
                readingBuffers[i] =
                        segment.wrap(
                                0,
                                (int) Math.min(requestedBytes - (long) i * bufferSize, bufferSize));
            }

            try {
                if (requestedBytes > 0) {
                    dataFileChannel.position(readingRequest.startOffset);
                    dataFileChannel.read(readingBuffers);
                }
            } catch (Throwable throwable) {
                buffers.addAll(readingSegments);
                failSubpartitionReaders(readingRequest.views, throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
                continue;
            }

            CompositeBuffer compositeBuffer = null;
            long currentBufferFileOffset = readingRequest.startOffset;
            long[] subpartitionBytes = new long[readingRequest.views.size()];
            Arrays.fill(subpartitionBytes, 0);

            for (int i = 0; i < numBuffers; ++i) {
                ByteBuffer currentBuffer = readingBuffers[i];
                currentBuffer.flip();

                MemorySegment currentSegment = readingSegments.get(i);
                int currentBufferBytes = currentBuffer.remaining();

                if (currentBufferBytes <= 0) {
                    buffers.add(currentSegment);
                    continue;
                }

                SharedBufferRecycler sharedRecycler = new SharedBufferRecycler(currentSegment);
                while (currentBuffer.hasRemaining()) {
                    if (headerBuffer.position() > 0) {
                        while (headerBuffer.hasRemaining()) {
                            headerBuffer.put(currentBuffer.get());
                        }
                        headerBuffer.flip();
                        currentHeader = parseBufferHeader(headerBuffer);
                        headerBuffer.clear();
                    }

                    if (currentHeader == null && currentBuffer.remaining() < HEADER_LENGTH) {
                        checkState(currentBuffer.position() > 0);
                        currentBufferFileOffset += HEADER_LENGTH;
                        headerBuffer.put(currentBuffer);
                        break;
                    } else if (currentHeader == null) {
                        currentBufferFileOffset += HEADER_LENGTH;
                        currentHeader = parseBufferHeader(currentBuffer);
                    }

                    if (compositeBuffer != null) {
                        int position = currentBuffer.position() + compositeBuffer.missingLength();
                        sharedRecycler.retain();
                        compositeBuffer.addPartialBuffer(
                                new NetworkBuffer(
                                                currentSegment,
                                                sharedRecycler,
                                                currentHeader.getDataType(),
                                                false,
                                                currentSegment.size())
                                        .readOnlySlice(
                                                currentBuffer.position(),
                                                compositeBuffer.missingLength()));
                        checkState(compositeBuffer.missingLength() == 0);
                        currentBuffer.position(position);
                    } else if (currentBuffer.remaining() < currentHeader.getLength()) {
                        compositeBuffer =
                                new CompositeBuffer(
                                        currentHeader.getDataType(),
                                        currentHeader.getLength(),
                                        currentHeader.isCompressed());
                        sharedRecycler.retain();
                        compositeBuffer.addPartialBuffer(
                                new NetworkBuffer(
                                                currentSegment,
                                                sharedRecycler,
                                                currentHeader.getDataType(),
                                                false,
                                                currentSegment.size())
                                        .readOnlySlice(
                                                currentBuffer.position(),
                                                currentBuffer.remaining()));
                        break;
                    } else {
                        compositeBuffer =
                                new CompositeBuffer(
                                        currentHeader.getDataType(),
                                        currentHeader.getLength(),
                                        currentHeader.isCompressed());
                        sharedRecycler.retain();
                        compositeBuffer.addPartialBuffer(
                                new NetworkBuffer(
                                                currentSegment,
                                                sharedRecycler,
                                                currentHeader.getDataType(),
                                                false,
                                                currentSegment.size())
                                        .readOnlySlice(
                                                currentBuffer.position(),
                                                currentHeader.getLength()));
                        currentBuffer.position(
                                currentBuffer.position() + currentHeader.getLength());
                    }

                    for (int viewIndex = 0; viewIndex < readingRequest.views.size(); ++viewIndex) {
                        SortMergeSubpartitionView view = readingRequest.views.get(viewIndex);
                        if (view == null) {
                            continue;
                        }
                        try {
                            SubpartitionReadingProgress readingProgress = view.getReadingProgress();
                            int length = currentHeader.getLength();
                            if (currentBufferFileOffset < readingProgress.getFileOffset()) {
                                break;
                            }
                            if (readingProgress.isDataInRange(currentBufferFileOffset, length)) {
                                compositeBuffer.retainBuffer();
                                Buffer buffer = compositeBuffer.duplicate();
                                subpartitionBytes[viewIndex] += length + HEADER_LENGTH;
                                view.addBuffer(buffer);
                            }
                        } catch (Throwable throwable) {
                            readingRequest.views.set(i, null);
                            failSubpartitionReaders(Collections.singletonList(view), throwable);
                            LOG.debug("Failed to read shuffle data.", throwable);
                        }
                    }
                    currentBufferFileOffset += currentHeader.getLength();
                    currentHeader = null;
                    compositeBuffer.recycleBuffer();
                    compositeBuffer = null;
                }
                sharedRecycler.recycle(currentSegment);
            }

            for (int i = 0; i < readingRequest.views.size(); ++i) {
                SortMergeSubpartitionView view = readingRequest.views.get(i);
                if (view == null) {
                    continue;
                }
                try {
                    if (!view.getReadingProgress()
                            .updateReadingProgress(subpartitionBytes[i], indexEntryBuf)) {
                        // there is no resource to release for finished readers currently
                        finishedViews.add(view);
                    }
                } catch (Throwable throwable) {
                    failSubpartitionReaders(Collections.singletonList(view), throwable);
                    LOG.debug("Failed to read shuffle data.", throwable);
                }
            }
            if (compositeBuffer != null) {
                compositeBuffer.recycleBuffer();
            }
        }
        return finishedViews;
    }

    private void failSubpartitionReaders(
            Collection<SortMergeSubpartitionView> views, Throwable failureCause) {
        synchronized (lock) {
            for (SortMergeSubpartitionView view : views) {
                if (view == null) {
                    continue;
                }
                failedViews.add(view);
            }
        }

        for (SortMergeSubpartitionView view : views) {
            if (view == null) {
                continue;
            }
            try {
                view.fail(failureCause);
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    private void removeFinishedAndFailedViews(
            int numBuffersRead, Set<SortMergeSubpartitionView> finishedViews) {
        synchronized (lock) {
            for (SortMergeSubpartitionView view : finishedViews) {
                allViews.remove(view);
            }
            finishedViews.clear();

            for (SortMergeSubpartitionView view : failedViews) {
                allViews.remove(view);
            }
            failedViews.clear();

            if (allViews.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannels();
            }

            numRequestedBuffers += numBuffersRead;
            isRunning = false;
            mayTriggerReading();
            mayNotifyReleased();
        }
    }

    private void mayNotifyReleased() {
        assert Thread.holdsLock(lock);

        if (isReleased && allViews.isEmpty()) {
            releaseFuture.complete(null);
        }
    }

    private List<ReadingRequest> computeReadingRequests() {
        ArrayList<SortMergeSubpartitionView> candidateViews = new ArrayList<>();
        synchronized (lock) {
            if (isReleased || allViews.isEmpty()) {
                return Collections.emptyList();
            }

            int regionIndex = Integer.MAX_VALUE;
            for (SortMergeSubpartitionView view : allViews) {
                int viewRegionIndex = view.getReadingProgress().getNextRegionIndex();
                if (viewRegionIndex < regionIndex) {
                    candidateViews.clear();
                    candidateViews.add(view);
                    regionIndex = viewRegionIndex;
                } else if (viewRegionIndex == regionIndex) {
                    candidateViews.add(view);
                }
            }
        }

        Collections.sort(candidateViews);
        ArrayList<ReadingRequest> readingRequests = new ArrayList<>();
        ReadingRequest currentReadingRequest = null;

        for (SortMergeSubpartitionView view : candidateViews) {
            SubpartitionReadingProgress readingProgress = view.getReadingProgress();
            if (currentReadingRequest == null
                    || currentReadingRequest.endOffset < readingProgress.getFileOffset()) {
                ReadingRequest readingRequest =
                        new ReadingRequest(
                                readingProgress.getFileOffset(),
                                readingProgress.getFileOffset()
                                        + readingProgress.getCurrentRegionRemainingBytes(),
                                new ArrayList<>(candidateViews.size()));
                readingRequests.add(readingRequest);
                currentReadingRequest = readingRequest;
            } else {
                long newEndOffset =
                        readingProgress.getFileOffset()
                                + readingProgress.getCurrentRegionRemainingBytes();
                if (newEndOffset > currentReadingRequest.endOffset) {
                    currentReadingRequest.updateEndOffset(newEndOffset);
                }
            }
            currentReadingRequest.addSubpartitionView(view);
        }
        return readingRequests;
    }

    SortMergeSubpartitionView createSubpartitionReader(
            BufferAvailabilityListener availabilityListener,
            int targetSubpartition,
            PartitionedFile resultFile)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "Partition is already released.");

            mayOpenFileChannels(resultFile);
            SortMergeSubpartitionView subpartitionView =
                    new SortMergeSubpartitionView(
                            availabilityListener,
                            new SubpartitionReadingProgress(
                                    resultFile, indexFileChannel, targetSubpartition));
            if (allViews.isEmpty()) {
                bufferPool.registerRequester(this);
            }
            allViews.add(subpartitionView);
            subpartitionView
                    .getReleaseFuture()
                    .thenRun(() -> releaseSubpartitionView(subpartitionView));

            mayTriggerReading();
            return subpartitionView;
        }
    }

    private void releaseSubpartitionView(SortMergeSubpartitionView subpartitionView) {
        synchronized (lock) {
            if (allViews.contains(subpartitionView)) {
                failedViews.add(subpartitionView);
            }
        }
    }

    private void mayOpenFileChannels(PartitionedFile resultFile) throws IOException {
        assert Thread.holdsLock(lock);

        try {
            if (allViews.isEmpty()) {
                openFileChannels(resultFile);
            }
        } catch (Throwable throwable) {
            if (allViews.isEmpty()) {
                closeFileChannels();
            }
            throw throwable;
        }
    }

    private void openFileChannels(PartitionedFile resultFile) throws IOException {
        assert Thread.holdsLock(lock);

        closeFileChannels();
        dataFileChannel = openFileChannel(resultFile.getDataFilePath());
        indexFileChannel = openFileChannel(resultFile.getIndexFilePath());
    }

    private void closeFileChannels() {
        assert Thread.holdsLock(lock);

        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
        dataFileChannel = null;
        indexFileChannel = null;
    }

    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;

            mayTriggerReading();
        }
    }

    private void mayTriggerReading() {
        assert Thread.holdsLock(lock);

        if (!isRunning
                && !allViews.isEmpty()
                && numRequestedBuffers + bufferPool.getNumBuffersPerRequest() <= maxRequestedBuffers
                && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
            isRunning = true;
            ioExecutor.execute(this);
        }
    }

    /**
     * Releases this read scheduler and returns a {@link CompletableFuture} which will be completed
     * when all resources are released.
     */
    CompletableFuture<?> release() {
        List<SortMergeSubpartitionView> pendingViews;
        synchronized (lock) {
            if (isReleased) {
                return releaseFuture;
            }
            isReleased = true;

            failedViews.addAll(allViews);
            pendingViews = new ArrayList<>(allViews);
            mayNotifyReleased();
        }

        failSubpartitionReaders(
                pendingViews,
                new IllegalStateException("Result partition has been already released."));
        return releaseFuture;
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    @VisibleForTesting
    int getNumPendingReaders() {
        synchronized (lock) {
            return allViews.size();
        }
    }

    @VisibleForTesting
    FileChannel getDataFileChannel() {
        synchronized (lock) {
            return dataFileChannel;
        }
    }

    @VisibleForTesting
    FileChannel getIndexFileChannel() {
        synchronized (lock) {
            return indexFileChannel;
        }
    }

    @VisibleForTesting
    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    @VisibleForTesting
    boolean isRunning() {
        synchronized (lock) {
            return isRunning;
        }
    }

    private static class ReadingRequest {

        private final long startOffset;

        private final ArrayList<SortMergeSubpartitionView> views;

        private long endOffset;

        private ReadingRequest(
                long startOffset, long endOffset, ArrayList<SortMergeSubpartitionView> views) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.views = checkNotNull(views);
        }

        void addSubpartitionView(SortMergeSubpartitionView view) {
            views.add(view);
        }

        void updateEndOffset(long newEndOffset) {
            endOffset = newEndOffset;
        }
    }

    private class SharedBufferRecycler implements BufferRecycler {

        private final AtomicInteger referenceCount = new AtomicInteger(1);

        public final MemorySegment segment;

        SharedBufferRecycler(MemorySegment segment) {
            this.segment = checkNotNull(segment);
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            checkArgument(memorySegment == segment);

            int count = referenceCount.decrementAndGet();
            if (count == 0) {
                SortMergeResultPartitionReader.this.recycle(memorySegment);
            } else if (count < 0) {
                throw new IllegalStateException("Illegal reference count.");
            }
        }

        void retain() {
            referenceCount.incrementAndGet();
        }
    }
}
