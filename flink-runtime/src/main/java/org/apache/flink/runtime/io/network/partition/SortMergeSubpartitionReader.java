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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Subpartition data reader for {@link SortMergeResultPartition}.
 */
public class SortMergeSubpartitionReader implements ResultSubpartitionView, Comparable<SortMergeSubpartitionReader> {

	private static final Logger LOG = LoggerFactory.getLogger(SortMergeSubpartitionReader.class);

	private final Object lock = new Object();

	/** Target {@link SortMergeResultPartition} to read data from. */
	private final SortMergeResultPartition partition;

	/** Listener to notify when data is available. */
	private final BufferAvailabilityListener availabilityListener;

	/** File reader used to read buffers from the shuffle data file. */
	private final PartitionedFileReader fileReader;

	/** Buffers already read which can be consumed by netty thread. */
	@GuardedBy("lock")
	private final Queue<Buffer> buffers = new ArrayDeque<>();

	/** Number of remaining non-event buffers in the buffer queue. */
	@GuardedBy("lock")
	private int dataBufferBacklog;

	/** Whether this reader is released or not. */
	@GuardedBy("lock")
	private boolean isReleased;

	/** Cause of failure which should be propagated to the consumer. */
	@GuardedBy("lock")
	private Throwable cause;

	/** Sequence number of the next buffer to be sent to the consumer. */
	private int sequenceNumber;

	public SortMergeSubpartitionReader(
			SortMergeResultPartition partition,
			BufferAvailabilityListener listener,
			PartitionedFileReader fileReader) {
		this.partition = checkNotNull(partition);
		this.availabilityListener = checkNotNull(listener);
		this.fileReader = checkNotNull(fileReader);
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() {
		synchronized (lock) {
			Buffer buffer = buffers.poll();
			if (buffer == null) {
				return null;
			}

			if (buffer.isBuffer()) {
				--dataBufferBacklog;
			}

			return BufferAndBacklog.fromBufferAndLookahead(buffer, buffers.peek(), dataBufferBacklog, sequenceNumber++);
		}
	}

	public void releaseFileReader() {
		try {
			fileReader.close();
		} catch (Throwable throwable) {
			LOG.info("Failed to close partitioned file reader.", throwable);
		}
	}

	public PartitionedFileReader getFileReader() {
		return fileReader;
	}

	public void addBuffer(Buffer buffer) {
		boolean notifyAvailable;
		synchronized (lock) {
			if (isReleased) {
				buffer.recycleBuffer();
				return;
			}

			notifyAvailable = buffers.isEmpty();

			buffers.add(buffer);
			if (buffer.isBuffer()) {
				++dataBufferBacklog;
			}
		}

		if (notifyAvailable) {
			notifyDataAvailable();
		}
	}

	@Override
	public void notifyDataAvailable() {
		availabilityListener.notifyDataAvailable();
	}

	public void fail(Throwable throwable) {
		synchronized (lock) {
			if (isReleased) {
				return;
			}

			if (cause == null) {
				cause = throwable;
			}
		}

		releaseAllResources();
		notifyDataAvailable();
	}

	@Override
	public int compareTo(SortMergeSubpartitionReader that) {
		long thisPriority = fileReader.getPriority();
		long thatPriority = that.fileReader.getPriority();

		if (thisPriority == thatPriority) {
			return 0;
		}

		return thisPriority > thatPriority ? 1 : -1;
	}

	@Override
	public void releaseAllResources() {
		synchronized (lock) {
			if (isReleased) {
				return;
			}

			for (Buffer buffer : buffers) {
				buffer.recycleBuffer();
			}
			buffers.clear();
			isReleased = true;
		}

		partition.releaseReader(this);
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
	public Throwable getFailureCause() {
		synchronized (lock) {
			return cause;
		}
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		synchronized (lock) {
			if (isReleased) {
				return true;
			}

			if (numCreditsAvailable > 0) {
				return !buffers.isEmpty();
			}

			return !buffers.isEmpty() && !buffers.peek().isBuffer();
		}
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}
}
