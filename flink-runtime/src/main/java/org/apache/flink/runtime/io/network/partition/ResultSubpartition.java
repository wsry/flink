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
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A single subpartition of a {@link AbstractResultPartition} instance.
 */
public abstract class ResultSubpartition {

	/** The info of the subpartition to identify it globally within a task. */
	protected final ResultSubpartitionInfo subpartitionInfo;

	/** The parent partition this subpartition belongs to. */
	protected final AbstractResultPartition parent;

	// - Statistics ----------------------------------------------------------

	public ResultSubpartition(int index, AbstractResultPartition parent) {
		this.parent = parent;
		this.subpartitionInfo = new ResultSubpartitionInfo(parent.getPartitionIndex(), index);
	}

	public ResultSubpartitionInfo getSubpartitionInfo() {
		return subpartitionInfo;
	}

	protected abstract long getTotalNumberOfBytes();

	public int getSubPartitionIndex() {
		return subpartitionInfo.getSubPartitionIdx();
	}

	/**
	 * Notifies the parent partition about a consumed {@link ResultSubpartitionView}.
	 */
	protected void onConsumedSubpartition() {
		parent.onConsumedSubpartition(getSubPartitionIndex());
	}

	public abstract void flush();

	public abstract void finish() throws IOException;

	public abstract void release() throws IOException;

	public abstract ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException;

	abstract int releaseMemory() throws IOException;

	public abstract boolean isReleased();

	/**
	 * Gets the number of non-event buffers in this subpartition.
	 *
	 * <p><strong>Beware:</strong> This method should only be used in tests in non-concurrent access
	 * scenarios since it does not make any concurrency guarantees.
	 */
	@VisibleForTesting
	abstract int getBuffersInBacklog();

	/**
	 * Makes a best effort to get the current size of the queue.
	 * This method must not acquire locks or interfere with the task and network threads in
	 * any way.
	 */
	public abstract int unsynchronizedGetNumberOfQueuedBuffers();

	// ------------------------------------------------------------------------

	/**
	 * A combination of a {@link Buffer} and the backlog length indicating
	 * how many non-event buffers are available in the subpartition.
	 */
	public static final class BufferAndBacklog {

		private final Buffer buffer;
		private final boolean isDataAvailable;
		private final int buffersInBacklog;
		private final boolean isEventAvailable;

		public BufferAndBacklog(Buffer buffer, boolean isDataAvailable, int buffersInBacklog, boolean isEventAvailable) {
			this.buffer = checkNotNull(buffer);
			this.buffersInBacklog = buffersInBacklog;
			this.isDataAvailable = isDataAvailable;
			this.isEventAvailable = isEventAvailable;
		}

		public Buffer buffer() {
			return buffer;
		}

		public boolean isDataAvailable() {
			return isDataAvailable;
		}

		public int buffersInBacklog() {
			return buffersInBacklog;
		}

		public boolean isEventAvailable() {
			return isEventAvailable;
		}

		public static BufferAndBacklog fromBufferAndLookahead(Buffer current, Buffer lookahead, int backlog) {
			return new BufferAndBacklog(
					current,
					lookahead != null,
					backlog,
					lookahead != null && !lookahead.isBuffer());
		}
	}

}
