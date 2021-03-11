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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fixed-size {@link MemorySegment} pool used by blocking shuffle for file writing and reading.
 */
@Internal
public class FileIOBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(FileIOBufferPool.class);

    public static final int MIN_TOTAL_BYTES = 16 * 1024 * 1024;

    public static final int MIN_BYTES_TO_REQUEST = 4 * 1024 * 1024;

    public static final int MAX_REQUESTED_BYTES = MIN_TOTAL_BYTES;

    private final Queue<MemorySegment> buffers = new ArrayDeque<>();

    @GuardedBy("buffers")
    private final Map<Object, Counter> numBuffersAllocated = new HashMap<>();

    private final int numTotalBuffers;

    private final int bufferSize;

    private final int numBuffersToRequest;

    private final int maxRequestedBuffers;

    @GuardedBy("buffers")
    private boolean destroyed;

    @GuardedBy("buffers")
    private boolean initialized;

    public FileIOBufferPool(long totalBytes, int bufferSize) {
        checkArgument(
                totalBytes >= MIN_TOTAL_BYTES,
                String.format(
                        "The configured memory size must be no smaller than 16M for %s.",
                        TaskManagerOptions.NETWORK_FILE_IO_MEMORY_SIZE.key()));
        checkArgument(
                totalBytes >= bufferSize,
                String.format(
                        "The configured value for %s must be no smaller than that for %s.",
                        TaskManagerOptions.NETWORK_FILE_IO_MEMORY_SIZE.key(),
                        TaskManagerOptions.MEMORY_SEGMENT_SIZE.key()));

        this.numTotalBuffers = (int) (totalBytes / bufferSize);
        this.bufferSize = bufferSize;
        this.numBuffersToRequest = Math.max(1, MIN_BYTES_TO_REQUEST / bufferSize);
        this.maxRequestedBuffers = Math.max(1, MAX_REQUESTED_BYTES / bufferSize);
    }

    public int getNumBuffersToRequest() {
        return numBuffersToRequest;
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getNumTotalBuffers() {
        return numTotalBuffers;
    }

    public void initialize() {
        LOG.info(
                "Initializing file IO buffer pool: numBuffers={}, bufferSize={}.",
                numTotalBuffers,
                bufferSize);

        synchronized (buffers) {
            if (initialized || destroyed) {
                return;
            }
            initialized = true;

            try {
                for (int i = 0; i < numTotalBuffers; ++i) {
                    buffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize));
                }
            } catch (OutOfMemoryError outOfMemoryError) {
                int allocated = buffers.size();
                buffers.clear();
                throw new OutOfMemoryError(
                        String.format(
                                "Can't initialize file IO buffer pool for blocking shuffle (bytes "
                                        + "already allocated: %d, bytes still needed: %d). Please "
                                        + "increase %s. Note that this buffer pool is initialized "
                                        + "when first used, the size of memory to be allocated is "
                                        + "configured by %s. You encounter this exception because "
                                        + "other components occupy too many direct memory.",
                                allocated * bufferSize,
                                (numTotalBuffers - allocated) * bufferSize,
                                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.NETWORK_FILE_IO_MEMORY_SIZE.key()));
            }
        }
    }

    public List<MemorySegment> requestBuffers(Object owner) throws InterruptedException {
        checkArgument(owner != null, "Owner must be not null.");

        List<MemorySegment> allocated = new ArrayList<>(numBuffersToRequest);
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            if (!initialized) {
                initialize();
            }

            Counter counter = numBuffersAllocated.get(owner);
            while (buffers.size() < numBuffersToRequest
                    || (counter != null
                            && counter.get() + numBuffersToRequest > maxRequestedBuffers)) {
                checkState(!destroyed, "Buffer pool is already destroyed.");

                buffers.wait();
                counter = numBuffersAllocated.get(owner);
            }

            while (allocated.size() < numBuffersToRequest) {
                allocated.add(buffers.poll());
            }

            if (counter == null) {
                counter = new Counter();
                numBuffersAllocated.put(owner, counter);
            }
            counter.increase(allocated.size());
        }
        return allocated;
    }

    public void recycle(MemorySegment segment, Object owner) {
        checkArgument(segment != null, "Buffer must be not null.");
        checkArgument(owner != null, "Owner must be not null.");

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            if (destroyed) {
                return;
            }

            decreaseCounter(1, owner);
            buffers.add(segment);

            if (buffers.size() >= numBuffersToRequest) {
                buffers.notifyAll();
            }
        }
    }

    public void recycle(Collection<MemorySegment> segments, Object owner) {
        checkArgument(segments != null, "Buffer list must be not null.");
        checkArgument(owner != null, "Owner must be not null.");

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            if (destroyed) {
                segments.clear();
                return;
            }

            decreaseCounter(segments.size(), owner);
            buffers.addAll(segments);

            if (buffers.size() >= numBuffersToRequest) {
                buffers.notifyAll();
            }
        }
    }

    private void decreaseCounter(int delta, Object owner) {
        assert Thread.holdsLock(buffers);

        Counter counter = checkNotNull(numBuffersAllocated.get(owner));
        counter.decrease(delta);
        if (counter.get() == 0) {
            numBuffersAllocated.remove(owner);
        }
    }

    public void destroy() {
        synchronized (buffers) {
            destroyed = true;

            buffers.clear();
            numBuffersAllocated.clear();

            buffers.notifyAll();
        }
    }

    private static final class Counter {

        private int counter = 0;

        int get() {
            return counter;
        }

        void increase(int delta) {
            counter += delta;
        }

        void decrease(int delta) {
            counter -= delta;
        }
    }
}
