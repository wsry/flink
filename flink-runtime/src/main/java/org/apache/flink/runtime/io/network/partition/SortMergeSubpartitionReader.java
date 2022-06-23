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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Subpartition data reader for {@link SortMergeResultPartition}. */
class SortMergeSubpartitionReader
        implements ResultSubpartitionView, Comparable<SortMergeSubpartitionReader> {

    private static final Logger LOG = LoggerFactory.getLogger(SortMergeSubpartitionReader.class);

    private final Object lock = new Object();

    /** A {@link CompletableFuture} to be completed when this subpartition reader is released. */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Listener to notify when data is available. */
    private final BufferAvailabilityListener availabilityListener;

    /** Buffers already read which can be consumed by netty thread. */
    @GuardedBy("lock")
    private final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /** File reader used to read buffer from. */
    private final PartitionedFileReader fileReader;

    /** Number of remaining non-event buffers in the buffer queue. */
    @GuardedBy("lock")
    private int dataBufferBacklog;

    /** Whether this reader is released or not. */
    @GuardedBy("lock")
    private boolean isReleased;

    /** Cause of failure which should be propagated to the consumer. */
    @GuardedBy("lock")
    private Throwable failureCause;

    /** Sequence number of the next buffer to be sent to the consumer. */
    private int sequenceNumber;

    private final boolean isBroadcastPartition;

    SortMergeSubpartitionReader(
            boolean isBroadcastPartition,
            BufferAvailabilityListener listener,
            PartitionedFileReader fileReader) {
        this.isBroadcastPartition = isBroadcastPartition;
        this.availabilityListener = checkNotNull(listener);
        this.fileReader = checkNotNull(fileReader);
    }

    SortMergeSubpartitionReader(
            BufferAvailabilityListener listener, PartitionedFileReader fileReader) {
        this(false, listener, fileReader);
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        synchronized (lock) {
            Buffer buffer = buffersRead.poll();
            if (buffer == null) {
                return null;
            }

            if (buffer.isBuffer()) {
                --dataBufferBacklog;
            }

            Buffer lookAhead = buffersRead.peek();
            return BufferAndBacklog.fromBufferAndLookahead(
                    buffer,
                    lookAhead == null ? Buffer.DataType.NONE : lookAhead.getDataType(),
                    dataBufferBacklog,
                    sequenceNumber++);
        }
    }

    void addBuffers(List<Buffer> buffers) {
        boolean notifyAvailable = false;
        boolean needRecycleBuffer = false;

        synchronized (lock) {
            if (isReleased) {
                needRecycleBuffer = true;
            } else {
                notifyAvailable = buffersRead.isEmpty();

                buffersRead.addAll(buffers);
                for (Buffer buffer : buffers) {
                    if (buffer.isBuffer()) {
                        ++dataBufferBacklog;
                    }
                }
            }
        }

        if (needRecycleBuffer) {
            buffers.forEach(Buffer::recycleBuffer);
            throw new IllegalStateException("Subpartition reader has been already released.");
        }

        if (notifyAvailable) {
            notifyDataAvailable();
        }
    }

    void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException {
        readBuffers(buffers, recycler, new BatchShuffleDataCache());
    }

    /** This method is called by the IO thread of {@link SortMergeResultPartitionReadScheduler}. */
    boolean readBuffers(
            Queue<MemorySegment> buffers, BufferRecycler recycler, BatchShuffleDataCache dataCache)
            throws IOException {
        long nextOffsetToRead = fileReader.getNextOffsetToRead();
        if (nextOffsetToRead == 0 && !dataCache.isEmpty()) {
            addBuffers(dataCache.getData());
            LOG.info("Reuse broadcast data: {}.", dataCache.size());
            return false;
        }

        while (!buffers.isEmpty()) {
            MemorySegment segment = buffers.poll();

            List<Buffer> buffersRead;
            try {
                if ((buffersRead = fileReader.readCurrentRegion(segment, recycler)) == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }

            if (!buffersRead.isEmpty()) {
                if (isBroadcastPartition && nextOffsetToRead == 0) {
                    dataCache.cacheData(buffersRead);
                }
                addBuffers(buffersRead);
            }
        }

        boolean hasRemaining = fileReader.hasRemaining();
        if (dataCache.isEmpty()) {
            LOG.info("No data cached: {}.", isBroadcastPartition);
            return hasRemaining;
        } else if (hasRemaining) {
            LOG.info("Clear data cached: {}.", dataCache.size());
            dataCache.clearCache().forEach(Buffer::recycleBuffer);
            return true;
        } else {
            LOG.info("Cache broadcast data: {}.", dataCache.size());
            return false;
        }
    }

    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    void fail(Throwable throwable) {
        checkArgument(throwable != null, "Must be not null.");

        releaseInternal(throwable);
        // notify the netty thread which will propagate the error to the consumer task
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public int compareTo(SortMergeSubpartitionReader that) {
        long thisPriority = fileReader.getNextOffsetToRead();
        long thatPriority = that.fileReader.getNextOffsetToRead();

        if (thisPriority == thatPriority) {
            return 0;
        }
        return thisPriority > thatPriority ? 1 : -1;
    }

    @Override
    public void releaseAllResources() {
        releaseInternal(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        List<Buffer> buffersToRecycle;
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            if (failureCause == null) {
                failureCause = throwable;
            }
            buffersToRecycle = new ArrayList<>(buffersRead);
            buffersRead.clear();
            dataBufferBacklog = 0;
        }
        buffersToRecycle.forEach(Buffer::recycleBuffer);
        buffersToRecycle.clear();

        releaseFuture.complete(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        synchronized (lock) {
            boolean isAvailable;
            if (isReleased) {
                isAvailable = true;
            } else if (buffersRead.isEmpty()) {
                isAvailable = false;
            } else {
                isAvailable = numCreditsAvailable > 0 || !buffersRead.peek().isBuffer();
            }
            return new AvailabilityWithBacklog(isAvailable, dataBufferBacklog);
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return Math.max(0, buffersRead.size());
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return buffersRead.size();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {}
}
