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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FileIOBufferPool;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Data reader for {@link SortMergeResultPartition} which can read data for all downstream tasks
 * consuming the corresponding {@link SortMergeResultPartition}. It always tries to read shuffle
 * data in order of file offset, which maximums the sequential read so can improve the blocking
 * shuffle performance.
 */
public class SortMergeResultPartitionReader implements Runnable, BufferRecycler {

    private static final Logger LOG = LoggerFactory.getLogger(SortMergeResultPartitionReader.class);

    /** Lock used to synchronize multi-thread access to thread-unsafe fields. */
    private final Object lock;

    private final FileIOBufferPool bufferPool;

    private final Executor ioExecutor;

    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionReader> failedReaders = new HashSet<>();

    /** All readers waiting to read data of different subpartitions. */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionReader> allReaders = new HashSet<>();

    /** File channel shared by all subpartitions to read data from. */
    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    /** File channel shared by all subpartitions to read index from. */
    @GuardedBy("lock")
    private FileChannel indexFileChannel;

    /** Whether this scheduler is released or not. */
    @GuardedBy("lock")
    private boolean isReleased;

    public SortMergeResultPartitionReader(
            FileIOBufferPool bufferPool, Executor ioExecutor, Object lock) {
        this.lock = checkNotNull(lock);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);

        bufferPool.initialize();
    }

    @Override
    public synchronized void run() {
        Set<SortMergeSubpartitionReader> finishedReaders = new HashSet<>();
        Queue<SortMergeSubpartitionReader> availableReaders = getAvailableReaders();

        Queue<MemorySegment> buffers = new ArrayDeque<>();
        try {
            buffers.addAll(bufferPool.requestBuffers(this));
        } catch (Throwable throwable) {
            // this can happen only when the buffer pool is destroyed so just release all readers
            for (SortMergeSubpartitionReader reader : availableReaders) {
                failSubpartitionReader(reader, throwable);
            }
            LOG.error("Failed to request buffers for data reading.", throwable);
        }

        Throwable firstException = null;
        while (!availableReaders.isEmpty() && !buffers.isEmpty()) {
            Throwable exception = null;
            SortMergeSubpartitionReader subpartitionReader = availableReaders.poll();

            try {
                if (!subpartitionReader.readBuffers(buffers, this)) {
                    // there is no resource to release for finished readers currently
                    finishedReaders.add(subpartitionReader);
                }
            } catch (Throwable throwable) {
                exception = throwable;
                firstException = firstException == null ? throwable : firstException;
                LOG.debug("Failed to read shuffle data.", throwable);
            }

            if (exception != null) {
                failSubpartitionReader(subpartitionReader, exception);
            }
        }

        if (buffers.size() > 0) {
            try {
                bufferPool.recycle(buffers, this);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just log the error
                LOG.error("Failed to release subpartition reader.", throwable);
            }
        }

        if (removeFinishedAndFailedReaders(finishedReaders, failedReaders) > 0) {
            ioExecutor.execute(this);
        }

        // only log the first exception to avoid too many logs for jobs of large parallelism
        if (firstException != null) {
            LOG.error("Encountered exception while reading shuffle data.", firstException);
        }
    }

    private void failSubpartitionReader(
            SortMergeSubpartitionReader reader, Throwable failureCause) {
        try {
            failedReaders.add(reader);
            reader.fail(failureCause);
        } catch (Throwable throwable) {
            // this should never happen so just log the error
            LOG.error("Failed to release subpartition reader.", throwable);
        }
    }

    private int removeFinishedAndFailedReaders(
            Set<SortMergeSubpartitionReader> finishedReaders,
            Set<SortMergeSubpartitionReader> failedReaders) {
        synchronized (lock) {
            for (SortMergeSubpartitionReader reader : finishedReaders) {
                allReaders.remove(reader);
            }
            finishedReaders.clear();

            for (SortMergeSubpartitionReader reader : failedReaders) {
                allReaders.remove(reader);
            }
            failedReaders.clear();

            if (allReaders.isEmpty()) {
                closeFileChannels();
            }
            return allReaders.size();
        }
    }

    private Queue<SortMergeSubpartitionReader> getAvailableReaders() {
        synchronized (lock) {
            if (isReleased) {
                failedReaders.addAll(allReaders);
                return new ArrayDeque<>();
            }

            return new PriorityQueue<>(allReaders);
        }
    }

    public SortMergeSubpartitionReader crateSubpartitionReader(
            SortMergeResultPartition resultPartition,
            BufferAvailabilityListener availabilityListener,
            int targetSubpartition,
            PartitionedFile resultFile)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "Partition is already released.");

            PartitionedFileReader fileReader = createFileReader(resultFile, targetSubpartition);
            SortMergeSubpartitionReader subpartitionReader =
                    new SortMergeSubpartitionReader(
                            resultPartition, availabilityListener, fileReader);
            allReaders.add(subpartitionReader);

            // submit data reading task when the first reader is created
            if (allReaders.size() == 1) {
                ioExecutor.execute(this);
            }
            return subpartitionReader;
        }
    }

    public void releaseSubpartitionReader(SortMergeSubpartitionReader subpartitionReader) {
        synchronized (lock) {
            if (allReaders.contains(subpartitionReader)) {
                failedReaders.add(subpartitionReader);
            }
        }
    }

    private PartitionedFileReader createFileReader(
            PartitionedFile resultFile, int targetSubpartition) throws IOException {
        assert Thread.holdsLock(lock);

        try {
            if (allReaders.isEmpty()) {
                openFileChannels(resultFile);
            }
            return new PartitionedFileReader(
                    resultFile, targetSubpartition, dataFileChannel, indexFileChannel);
        } catch (Throwable throwable) {
            if (allReaders.isEmpty()) {
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
        bufferPool.recycle(segment, this);
    }

    public void release() {
        synchronized (lock) {
            isReleased = true;
        }
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }
}
