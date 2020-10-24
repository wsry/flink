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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Reader which can read all data of the target subpartition from a {@link PartitionedFile}.
 */
public class PartitionedFileReader implements AutoCloseable {

	/** Used to read buffers from file channel. */
	private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

	/** Used to read index entry from index file. */
	private final ByteBuffer indexEntryBuf;

	/** Target {@link PartitionedFile} to read. */
	private final PartitionedFile partitionedFile;

	/** Target subpartition to read. */
	private final int targetSubpartition;

	/** Data file channel of the target {@link PartitionedFile}. */
	private final FileChannel dataFileChannel;

	/** Index file channel of the target {@link PartitionedFile}. */
	private final FileChannel indexFileChannel;

	/** Next data region to be read. */
	private int nextRegionToRead;

	/** Next offset in the data file to read. */
	private long nextReadOffset;

	/** Number of remaining buffers in the current data region read. */
	private int currentRegionRemainingBuffers;

	/** Whether this partitioned file reader is closed. */
	private boolean isClosed;

	public PartitionedFileReader(
			PartitionedFile partitionedFile,
			int targetSubpartition,
			FileChannel dataFileChannel,
			FileChannel indexFileChannel) throws IOException {
		checkArgument(dataFileChannel.isOpen() && indexFileChannel.isOpen(), "File channels must be opened.");

		this.partitionedFile = checkNotNull(partitionedFile);
		this.targetSubpartition = targetSubpartition;
		this.dataFileChannel = checkNotNull(dataFileChannel);
		this.indexFileChannel = checkNotNull(indexFileChannel);

		this.indexEntryBuf = ByteBuffer.allocate(PartitionedFile.INDEX_ENTRY_SIZE);
		indexEntryBuf.order(PartitionedFile.DEFAULT_BYTE_ORDER);

		moveToNextReadableRegion();
	}

	public boolean moveToNextReadableRegion() throws IOException {
		while (currentRegionRemainingBuffers == 0 && nextRegionToRead < partitionedFile.getNumRegions()) {
			partitionedFile.getIndexEntry(indexFileChannel, indexEntryBuf, nextRegionToRead, targetSubpartition);
			nextReadOffset = indexEntryBuf.getLong();
			currentRegionRemainingBuffers = indexEntryBuf.getInt();

			++nextRegionToRead;
		}

		return currentRegionRemainingBuffers > 0;
	}

	/**
	 * Reads a buffer from the {@link PartitionedFile} and moves the read position forward.
	 *
	 * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
	 */
	@Nullable
	public Buffer readBuffer(MemorySegment target, BufferRecycler recycler) throws IOException {
		Buffer buffer = null;
		if (moveToNextReadableRegion()) {
			dataFileChannel.position(nextReadOffset);
			buffer = BufferReaderWriterUtil.readFromByteChannel(dataFileChannel, headerBuf, target, recycler);
			nextReadOffset = dataFileChannel.position();
			--currentRegionRemainingBuffers;
		}

		return buffer;
	}

	public long getNextReadOffset() {
		return nextReadOffset;
	}

	public int getCurrentRegionRemainingBuffers() {
		return currentRegionRemainingBuffers;
	}

	/** Gets read priority of this file reader. Smaller value indicates higher priority. */
	public long getPriority() {
		return nextReadOffset;
	}

	@VisibleForTesting
	public boolean hasRemaining() throws IOException {
		return moveToNextReadableRegion();
	}

	@Override
	public void close() {
		if (isClosed) {
			return;
		}
		isClosed = true;
	}
}
