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

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Reader which can read all data of the target subpartition from a {@link PartitionedFile}.
 */
public class PartitionedFileReader implements AutoCloseable {

	/** Used to read buffers from file channel. */
	private final ByteBuffer headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

	/** Target {@link PartitionedFile} to read. */
	private final PartitionedFile partitionedFile;

	/** Target subpartition to read. */
	private final int subpartition;

	/** Opened file channel of the target {@link PartitionedFile}. */
	private FileChannel dataFileChannel;

	/** Number of remaining buffers to read of the target subpartition. */
	private int numReamingBuffers;

	/** Whether this partitioned file reader is closed. */
	private boolean isClosed;

	public PartitionedFileReader(PartitionedFile partitionedFile, int subpartitionIndex) {
		this.partitionedFile = checkNotNull(partitionedFile);
		this.subpartition = subpartitionIndex;
		this.numReamingBuffers = partitionedFile.getNumBuffers(subpartitionIndex);
	}

	/**
	 * Opens the given {@link PartitionedFile} and moves read position to the starting offset of the
	 * target subpartition.
	 */
	public void open() throws IOException {
		checkState(dataFileChannel == null, "File reader is already opened.");
		checkState(!isClosed, "File reader is already closed.");

		try {
			dataFileChannel = FileChannel.open(partitionedFile.getDataFile(), StandardOpenOption.READ);
			dataFileChannel.position(partitionedFile.getStartingOffset(subpartition));
		} catch (Throwable throwable) {
			close();
			throw throwable;
		}
	}

	/**
	 * Reads a buffer from the {@link PartitionedFile} and moves the read position forward.
	 *
	 * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
	 */
	@Nullable
	public Buffer readBuffer(MemorySegment target, BufferRecycler recycler) throws IOException {
		checkState(dataFileChannel != null, "Must open the partitioned file first.");
		checkState(!isClosed, "File reader is already closed.");

		if (!hasRemaining()) {
			return null;
		}

		--numReamingBuffers;
		return BufferReaderWriterUtil.readFromByteChannel(dataFileChannel, headerBuffer, target, recycler);
	}

	public boolean hasRemaining() {
		return numReamingBuffers > 0;
	}

	@Override
	public void close() throws IOException {
		checkState(!isClosed, "File reader is already closed.");

		isClosed = true;

		if (dataFileChannel != null) {
			dataFileChannel.close();
		}
	}
}
