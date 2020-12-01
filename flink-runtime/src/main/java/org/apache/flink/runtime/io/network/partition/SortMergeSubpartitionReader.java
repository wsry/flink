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
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Subpartition data reader for {@link SortMergeResultPartition}.
 */
public class SortMergeSubpartitionReader implements ResultSubpartitionView {

	/** Target {@link SortMergeResultPartition} to read data from. */
	private final SortMergeResultPartition partition;

	/** Listener to notify when data is available. */
	private final BufferAvailabilityListener availabilityListener;

	/** File reader used to read buffer from. */
	private final PartitionedFileReader fileReader;

	/** Number of remaining non-event buffers to read. */
	private int dataBufferBacklog;

	/** Number of remaining data and event buffers to read. */
	private int dataAndEventBufferBacklog;

	/** Whether this reader is released or not. */
	private boolean isReleased;

	/** Sequence number of the next buffer to be sent to the consumer. */
	private int sequenceNumber;

	public SortMergeSubpartitionReader(
			int subpartitionIndex,
			int dataBufferBacklog,
			int dataAndEventBufferBacklog,
			SortMergeResultPartition partition,
			BufferAvailabilityListener listener,
			PartitionedFile partitionedFile) throws IOException {
		this.partition = checkNotNull(partition);
		this.availabilityListener = checkNotNull(listener);
		this.dataBufferBacklog = dataBufferBacklog;
		this.dataAndEventBufferBacklog = dataAndEventBufferBacklog;
		this.fileReader = new PartitionedFileReader(partitionedFile, subpartitionIndex);
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException {
		if (isReleased) {
			return null;
		}

		Buffer buffer = fileReader.readBuffer();
		if (buffer == null) {
			return null;
		}

		--dataAndEventBufferBacklog;
		if (buffer.isBuffer()) {
			--dataBufferBacklog;
		}

		return BufferAndBacklog.fromBufferAndLookahead(
			buffer,
			dataAndEventBufferBacklog > 0 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.NONE,
			dataBufferBacklog,
			sequenceNumber++);
	}

	@Override
	public void notifyDataAvailable() {
		availabilityListener.notifyDataAvailable();
	}

	@Override
	public void releaseAllResources() {
		isReleased = true;

		IOUtils.closeQuietly(fileReader);
		partition.releaseReader(this);
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public void resumeConsumption() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public Throwable getFailureCause() {
		// we can never throw an error after this was created
		return null;
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		return numCreditsAvailable > 0 && dataAndEventBufferBacklog > 0;
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}
}
