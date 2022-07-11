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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

    /**
     * NUmber of data buffers can be read per IO request (128 KB by default). Reading too much data
     * per request may break the data process Pipeline.
     */
    private static final int NUM_BUFFER_PER_READ = 4;

    /**
     * Default maximum time (5min) to wait when requesting read buffers from the buffer pool before
     * throwing an exception.
     */
    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    /** Lock used to synchronize multi-thread access to thread-unsafe fields. */
    private final Object lock;

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /** Used to temporarily store and parse partial buffer header. */
    private final ByteBuffer headerBuf;

    /** Size of memory buffer for data reading. */
    private final int bufferSize;

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

    /** All failed subpartition views to be released. */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionView> failedViews = new HashSet<>();

    /** All subpartition views waiting to read data of subpartition data. */
    @GuardedBy("lock")
    private final HashSet<SortMergeSubpartitionView> allViews = new HashSet<>();

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
        this.bufferSize = bufferPool.getBufferSize();
        // one partition reader can consume at most Math.max(16M, numSubpartitions) (the expected
        // buffers per request is 8M) buffers for data read, which means larger parallelism, more
        // buffers. Currently, it is only an empirical strategy which can not be configured.
        this.maxRequestedBuffers =
                Math.max(2 * bufferPool.getMaxBuffersPerRequest(), numSubpartitions);
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);

        this.headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();
        this.indexEntryBuf = BufferReaderWriterUtil.allocateIndexEntryBuffer();
    }

    @Override
    public synchronized void run() {
        List<ReadingRequest> readingRequests;
        try {
            readingRequests = computeReadingRequests();
            checkState(!readingRequests.isEmpty(), "No valid shuffle data reading request.");
        } catch (Throwable throwable) {
            // fail all pending subpartition readers immediately if any exception occurs
            LOG.error("Failed to merge shuffle data reading requests.", throwable);
            failSubpartitionViews(allViews(), throwable);
            removeFinishedAndFailedViews(0, new HashSet<>());
            return;
        }

        int numBuffersRequired = 0;
        for (ReadingRequest readingRequest : readingRequests) {
            numBuffersRequired += readingRequest.maxBuffersRequired;
        }

        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers(numBuffersRequired);
        } catch (Throwable throwable) {
            // fail all pending subpartition readers immediately if any exception occurs
            LOG.error("Failed to request buffers for shuffle data reading.", throwable);
            failSubpartitionViews(allViews(), throwable);
            removeFinishedAndFailedViews(0, new HashSet<>());
            return;
        }

        int numBuffersAllocated = buffers.size();
        Set<SortMergeSubpartitionView> finishedViews =
                processReadingRequests(readingRequests, buffers);
        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);

        removeFinishedAndFailedViews(numBuffersRead, finishedViews);
    }

    @VisibleForTesting
    Queue<MemorySegment> allocateBuffers(int numBuffersToRequest) throws Exception {
        if (numBuffersToRequest <= 0) {
            return new ArrayDeque<>();
        }
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers(numBuffersToRequest);
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

    private ArrayList<SortMergeSubpartitionView> allViews() {
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

    private MemorySegment[] prepareReadingSegments(
            long numRequestBytes, Queue<MemorySegment> segments) {
        int numBuffers = Math.min(NUM_BUFFER_PER_READ, segments.size());
        if (numRequestBytes % bufferSize != 0) {
            numBuffers = (int) Math.min(numBuffers, numRequestBytes / bufferSize + 1);
        } else {
            numBuffers = (int) Math.min(numBuffers, numRequestBytes / bufferSize);
        }

        MemorySegment[] readingSegments = new MemorySegment[numBuffers];
        for (int i = 0; i < numBuffers; ++i) {
            readingSegments[i] = segments.poll();
        }
        return readingSegments;
    }

    private ByteBuffer[] prepareReadingBuffers(long numRequestBytes, MemorySegment[] segments) {
        ByteBuffer[] byteBuffers = new ByteBuffer[segments.length];
        for (int i = 0; i < segments.length; ++i) {
            MemorySegment segment = segments[i];
            byteBuffers[i] = segment.wrap(0, (int) Math.min(numRequestBytes, bufferSize));
            numRequestBytes -= byteBuffers[i].limit();
        }
        return byteBuffers;
    }

    private void addBufferToSubpartitionViews(
            long fileOffsetOfBuffer,
            CompositeBuffer compositeBuffer,
            ReadingRequest readingRequest,
            BufferHeader bufferHeader,
            Set<SortMergeSubpartitionView> finishedViews) {
        for (int i = readingRequest.startViewIndex; i < readingRequest.views.size(); ++i) {
            SortMergeSubpartitionView view = readingRequest.views.get(i);
            if (view == null) {
                continue;
            }
            try {
                SubpartitionReadingProgress readingProgress = view.getReadingProgress();
                int length = bufferHeader.getLength() + HEADER_LENGTH;
                if (fileOffsetOfBuffer < readingProgress.getCurrentStartOffset()) {
                    break;
                }

                if (fileOffsetOfBuffer + length > readingProgress.getCurrentEndOffset()) {
                    readingRequest.updateCurrentViewIndex(i + 1);
                    continue;
                }

                view.addBuffer(compositeBuffer.duplicate());
                if (!view.updateReadingProgress(length, indexEntryBuf)) {
                    // there is no resource to release for finished readers currently
                    finishedViews.add(view);
                }
            } catch (Throwable throwable) {
                readingRequest.views.set(i, null);
                failSubpartitionViews(Collections.singletonList(view), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
    }

    @Nullable
    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        if (headerBuf.position() > 0) {
            while (headerBuf.hasRemaining()) {
                headerBuf.put(buffer.get());
            }
            headerBuf.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(headerBuf);
            headerBuf.clear();
        }

        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            headerBuf.put(buffer);
        } else if (header == null) {
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
    }

    private BufferAndHeader processBuffer(
            long fileOffsetOfBuffer,
            ByteBuffer byteBuffer,
            MemorySegment segment,
            BufferHeader header,
            CompositeBuffer targetBuffer,
            ReferenceCountedRecycler recycler,
            ReadingRequest readingRequest,
            Set<SortMergeSubpartitionView> finishedViews) {
        while (byteBuffer.hasRemaining()) {
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            NetworkBuffer networkBuffer =
                    new NetworkBuffer(segment, recycler, header.getDataType(), false, bufferSize);
            recycler.retain();
            if (targetBuffer != null) {
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        networkBuffer.readOnlySlice(
                                byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
            } else if (byteBuffer.remaining() < header.getLength()) {
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        networkBuffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                break;
            } else {
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        networkBuffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            addBufferToSubpartitionViews(
                    fileOffsetOfBuffer, targetBuffer, readingRequest, header, finishedViews);
            fileOffsetOfBuffer += HEADER_LENGTH + header.getLength();
            header = null;
            targetBuffer.recycleBuffer();
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header, fileOffsetOfBuffer);
    }

    private BufferAndHeader processBuffers(
            BufferAndHeader bufferAndHeader,
            ReadingRequest readingRequest,
            MemorySegment[] segments,
            ByteBuffer[] byteBuffers,
            Set<SortMergeSubpartitionView> finishedViews) {
        for (int i = 0; i < byteBuffers.length; ++i) {
            ByteBuffer byteBuffer = byteBuffers[i];
            byteBuffer.flip();
            MemorySegment segment = segments[i];

            if (byteBuffer.remaining() <= 0) {
                bufferPool.recycle(segment);
                continue;
            }

            ReferenceCountedRecycler recycler = new ReferenceCountedRecycler(segment);
            bufferAndHeader =
                    processBuffer(
                            bufferAndHeader.fileOffsetOfBuffer,
                            byteBuffer,
                            segment,
                            bufferAndHeader.header,
                            bufferAndHeader.buffer,
                            recycler,
                            readingRequest,
                            finishedViews);
            recycler.recycle(segment);
        }
        return bufferAndHeader;
    }

    private boolean setFileOffset(long offset, ReadingRequest readingRequest) {
        try {
            dataFileChannel.position(offset);
            return true;
        } catch (Throwable throwable) {
            failSubpartitionViews(readingRequest.views, throwable);
            LOG.error("Failed to read shuffle data.", throwable);
            return false;
        }
    }

    private long readFileData(ByteBuffer[] byteBuffers, ReadingRequest readingRequest) {
        try {
            return dataFileChannel.read(byteBuffers);
        } catch (Throwable throwable) {
            failSubpartitionViews(readingRequest.views, throwable);
            LOG.error("Failed to read shuffle data.", throwable);
            return 0;
        }
    }

    private Set<SortMergeSubpartitionView> processReadingRequests(
            List<ReadingRequest> readingRequests, Queue<MemorySegment> buffers) {
        headerBuf.clear();
        Set<SortMergeSubpartitionView> finishedViews = new HashSet<>();

        for (ReadingRequest readingRequest : readingRequests) {
            if (buffers.isEmpty()) {
                break;
            }

            long fileOffsetOfBuffer = readingRequest.startOffset;
            if (!setFileOffset(fileOffsetOfBuffer, readingRequest)) {
                continue;
            }

            BufferAndHeader bufferAndHeader = new BufferAndHeader(null, null, fileOffsetOfBuffer);
            long numRequestBytes = readingRequest.getRequestBytes();

            while (!buffers.isEmpty() && numRequestBytes > 0) {
                MemorySegment[] segments = prepareReadingSegments(numRequestBytes, buffers);
                ByteBuffer[] byteBuffers = prepareReadingBuffers(numRequestBytes, segments);

                long numBytesRead = readFileData(byteBuffers, readingRequest);
                if (numBytesRead <= 0) {
                    buffers.addAll(Arrays.asList(segments));
                    break;
                }
                numRequestBytes -= numBytesRead;
                bufferAndHeader =
                        processBuffers(
                                bufferAndHeader,
                                readingRequest,
                                segments,
                                byteBuffers,
                                finishedViews);
            }
            if (bufferAndHeader != null && bufferAndHeader.buffer != null) {
                bufferAndHeader.buffer.recycleBuffer();
            }
        }
        return finishedViews;
    }

    private void failSubpartitionViews(
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

    /**
     * This method merges the subpartition reading requests which read the same data together and
     * returns the merged requests. One {@link ReadingRequest} will be served by one file read.
     */
    private List<ReadingRequest> computeReadingRequests() throws IOException {
        ArrayList<SortMergeSubpartitionView> subpartitionViewCandidates;
        synchronized (lock) {
            if (isReleased || allViews.isEmpty()) {
                return Collections.emptyList();
            }

            subpartitionViewCandidates = new ArrayList<>(allViews.size());
            int currentRegionIndex = Integer.MAX_VALUE;
            for (SortMergeSubpartitionView view : allViews) {
                if (!view.getReadingProgress().currentRegionHasRemaining()
                        && !view.updateReadingProgress(0, indexEntryBuf)) {
                    continue;
                }

                int regionIndex = view.getReadingProgress().getNextRegionIndex();
                if (regionIndex < currentRegionIndex) {
                    subpartitionViewCandidates.clear();
                    subpartitionViewCandidates.add(view);
                    currentRegionIndex = regionIndex;
                } else if (regionIndex == currentRegionIndex) {
                    subpartitionViewCandidates.add(view);
                }
            }
        }
        return mergeSubpartitionReadings(subpartitionViewCandidates);
    }

    private List<ReadingRequest> mergeSubpartitionReadings(
            ArrayList<SortMergeSubpartitionView> subpartitionViewCandidates) {
        Collections.sort(subpartitionViewCandidates);
        ArrayList<ReadingRequest> readingRequests = new ArrayList<>();
        ReadingRequest currentReadingRequest = new ReadingRequest(0, new ArrayList<>());

        int currentBuffersRequired = 0;
        for (SortMergeSubpartitionView view : subpartitionViewCandidates) {
            SubpartitionReadingProgress readingProgress = view.getReadingProgress();
            if (currentReadingRequest.endOffset < readingProgress.getCurrentEndOffset()
                    && bufferPool.getMaxBuffersPerRequest()
                            < currentBuffersRequired + currentReadingRequest.maxBuffersRequired) {
                break;
            }

            if (currentReadingRequest.endOffset < readingProgress.getCurrentStartOffset()) {
                currentBuffersRequired += currentReadingRequest.maxBuffersRequired;
                ReadingRequest readingRequest =
                        new ReadingRequest(
                                readingProgress.getCurrentStartOffset(),
                                new ArrayList<>(subpartitionViewCandidates.size()));
                readingRequests.add(readingRequest);
                currentReadingRequest = readingRequest;
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

            if (allViews.isEmpty()) {
                openFileChannels(resultFile);
            }
            SortMergeSubpartitionView view =
                    new SortMergeSubpartitionView(
                            availabilityListener,
                            new SubpartitionReadingProgress(
                                    resultFile, indexFileChannel, targetSubpartition));
            if (allViews.isEmpty()) {
                bufferPool.registerRequester(this);
            }
            allViews.add(view);
            view.getReleaseFuture().thenRun(() -> releaseSubpartitionView(view));

            mayTriggerReading();
            return view;
        }
    }

    private void releaseSubpartitionView(SortMergeSubpartitionView subpartitionView) {
        synchronized (lock) {
            if (allViews.contains(subpartitionView)) {
                failedViews.add(subpartitionView);
            }
        }
    }

    private void openFileChannels(PartitionedFile resultFile) throws IOException {
        assert Thread.holdsLock(lock);
        if (!allViews.isEmpty()) {
            return;
        }

        try {
            closeFileChannels();
            dataFileChannel = openFileChannel(resultFile.getDataFilePath());
            indexFileChannel = openFileChannel(resultFile.getIndexFilePath());
        } catch (Throwable throwable) {
            closeFileChannels();
            throw throwable;
        }
    }

    private void closeFileChannels() {
        assert Thread.holdsLock(lock);
        if (!allViews.isEmpty()) {
            return;
        }

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
                && numRequestedBuffers + bufferPool.getMaxBuffersPerRequest() <= maxRequestedBuffers
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

        failSubpartitionViews(
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

    private class ReadingRequest {

        private final long startOffset;
        private final ArrayList<SortMergeSubpartitionView> views;
        private long endOffset = -1;
        private int maxBuffersRequired;
        private int startViewIndex;

        ReadingRequest(long startOffset, ArrayList<SortMergeSubpartitionView> views) {
            this.startOffset = startOffset;
            this.views = checkNotNull(views);
        }

        void addSubpartitionView(SortMergeSubpartitionView view) {
            long newEndOffset = view.getReadingProgress().getCurrentEndOffset();
            if (newEndOffset > endOffset) {
                endOffset = newEndOffset;
                long numRequestBytes = getRequestBytes();
                maxBuffersRequired =
                        MathUtils.checkedDownCast(
                                numRequestBytes / bufferSize
                                        + (numRequestBytes % bufferSize > 0 ? 1 : 0));
            }
            views.add(view);
        }

        long getRequestBytes() {
            return endOffset - startOffset;
        }

        void updateCurrentViewIndex(int newViewIndex) {
            startViewIndex = newViewIndex;
        }
    }

    private static class BufferAndHeader {

        private final CompositeBuffer buffer;
        private final BufferHeader header;
        private final long fileOffsetOfBuffer;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header, long fileOffsetOfBuffer) {
            this.buffer = buffer;
            this.header = header;
            this.fileOffsetOfBuffer = fileOffsetOfBuffer;
        }
    }

    private class ReferenceCountedRecycler implements BufferRecycler {

        private final MemorySegment segment;
        private final AtomicInteger referenceCount = new AtomicInteger(1);

        ReferenceCountedRecycler(MemorySegment segment) {
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
