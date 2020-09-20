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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Subpartition data reader for {@link SortMergeResultPartition}.
 */
public class SortMergePartitionReader implements ResultSubpartitionView, BufferRecycler {

	/** Target {@link SortMergeResultPartition} to read data from. */
	private final SortMergeResultPartition partition;

	/** Listener to notify when data is available. */
	private final BufferAvailabilityListener availabilityListener;

	/** All {@link PartitionedFile}s to read. */
	private final Queue<PartitionedFile> partitionedFiles;

	/** Unmanaged memory used as read buffers. */
	private final BlockingQueue<MemorySegment> readBuffers = new LinkedBlockingQueue<>();

	/** Target subpartition to read. */
	private final int subpartitionIndex;

	/** Number of remaining non-event buffers to read. */
	private int dataBufferBacklog;

	/** Current {@link PartitionedFileReader} to read buffer from. */
	private PartitionedFileReader currentFileReader;

	/** Next buffer used to judge whether the next one is an event. */
	private Buffer nextBuffer;

	/** Whether this reader is released or not. */
	private boolean isReleased;

	/** Sequence number of the next buffer to be sent to the consumer. */
	private int sequenceNumber;

	public SortMergePartitionReader(
			SortMergeResultPartition partition,
			Queue<PartitionedFile> partitionedFiles,
			BufferAvailabilityListener listener,
			int subpartitionIndex,
			int dataBufferBacklog,
			int bufferSize) throws IOException {
		checkArgument(!partitionedFiles.isEmpty(), "No partitioned file to read.");

		this.partition = checkNotNull(partition);
		this.partitionedFiles = checkNotNull(partitionedFiles);
		this.availabilityListener = checkNotNull(listener);
		this.dataBufferBacklog = dataBufferBacklog;
		this.subpartitionIndex = subpartitionIndex;

		// allocate two pieces of unmanaged segments for data reading
		for (int i = 0; i < 2; i++) {
			readBuffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize, null));
		}
		nextBuffer = readNextBuffer();
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException {
		checkState(!isReleased, "Reader is already released.");
		checkState(nextBuffer != null, "Next buffer should not be null.");

		Buffer buffer = nextBuffer;
		if (buffer.isBuffer()) {
			--dataBufferBacklog;
		}

		nextBuffer = readNextBuffer();
		return BufferAndBacklog.fromBufferAndLookahead(buffer, nextBuffer, dataBufferBacklog, sequenceNumber++);
	}

	private Buffer readNextBuffer() throws IOException {
		MemorySegment segment = readBuffers.poll();
		if (segment == null) {
			return null;
		}

		// we do not need to recycle the allocated segment here if any exception occurs
		// for this subpartition reader will be released so no resource will be leaked
		Buffer buffer = null;
		while (buffer == null && !partitionedFiles.isEmpty()) {
			if (currentFileReader == null) {
				currentFileReader = new PartitionedFileReader(partitionedFiles.peek(), subpartitionIndex);
				currentFileReader.open();
			}

			buffer = currentFileReader.readBuffer(segment, this);
			if (!currentFileReader.hasRemaining()) {
				currentFileReader.close();
				partitionedFiles.remove();
				currentFileReader = null;
			}
		}
		return buffer;
	}

	@Override
	public void notifyDataAvailable() {
		if (nextBuffer != null) {
			availabilityListener.notifyDataAvailable();
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		readBuffers.add(segment);

		// notify data available if the reader is unavailable currently
		if (!isReleased && nextBuffer == null) {
			try {
				nextBuffer = readNextBuffer();
			} catch (IOException exception) {
				ExceptionUtils.rethrow(exception, "Failed to read next buffer.");
			}
			notifyDataAvailable();
		}
	}

	@Override
	public void releaseAllResources() {
		isReleased = true;

		IOUtils.closeQuietly(currentFileReader);
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
		if (numCreditsAvailable > 0) {
			return nextBuffer != null;
		}

		return nextBuffer != null && !nextBuffer.isBuffer();
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}
}
