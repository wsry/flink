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
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File writer to write buffers to {@link PartitionedFile} in subpartition order.
 */
@NotThreadSafe
public class PartitionedFileWriter {

	public static final String DATA_FILE_SUFFIX = ".data";

	/** Used when writing data buffers. */
	private final ByteBuffer[] header;

	/** Number of channels. When writing a buffer, target subpartition must be in this range. */
	private final int numSubpartitions;

	/** Number of bytes written for each subpartition. */
	private final long[] numSubpartitionBytes;

	/** Number of buffers written for each subpartition. */
	private final int[] numSubpartitionBuffers;

	/** Path of the target {@link PartitionedFile}. */
	private final Path filePath;

	/** Opened file channel of the target {@link PartitionedFile}. */
	private FileChannel fileChannel;

	/** Current subpartition to write. Buffer writing must be in subpartition order. */
	private int currentSubpartition;

	/** Whether this file writer is finished. */
	private boolean isFinished;

	public PartitionedFileWriter(String basePath, int numSubpartitions) {
		checkArgument(basePath != null, "Base path must not be null.");
		checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");

		this.numSubpartitions = numSubpartitions;

		this.header = BufferReaderWriterUtil.allocatedWriteBufferArray();
		this.numSubpartitionBytes = new long[numSubpartitions];
		this.numSubpartitionBuffers = new int[numSubpartitions];

		this.filePath = new File(basePath + DATA_FILE_SUFFIX).toPath();
	}

	/**
	 * Opens a {@link PartitionedFile} for writing.
	 */
	public void open() throws IOException {
		checkState(fileChannel == null, "Partitioned file is already opened.");

		fileChannel = FileChannel.open(filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
	}

	/**
	 * Writes a {@link Buffer} of the given subpartition to the opened {@link PartitionedFile}.
	 *
	 * <p>Note: The caller is responsible for recycling the target buffer and releasing the failed
	 * {@link PartitionedFile} if any exception occurs.
	 */
	public void writeBuffer(Buffer target, int subpartitionIndex) throws IOException {
		checkArgument(subpartitionIndex >= currentSubpartition, "Must write in subpartition index order.");
		checkState(!isFinished, "File writer is already finished.");
		checkState(fileChannel != null, "Must open the partitioned file first.");

		currentSubpartition = Math.max(currentSubpartition, subpartitionIndex);
		long numBytes = BufferReaderWriterUtil.writeToByteChannel(fileChannel, target, header);
		++numSubpartitionBuffers[subpartitionIndex];
		numSubpartitionBytes[subpartitionIndex] += numBytes;
	}

	/**
	 * Finishes the current {@link PartitionedFile} which closes the file channel and constructs
	 * the corresponding {@link PartitionedFile.PartitionedFileIndex}.
	 *
	 * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
	 * exception occurs.
	 */
	public PartitionedFile finish() throws IOException {
		checkState(!isFinished, "File writer is already finished.");
		checkState(fileChannel != null, "Must open the partitioned file first.");

		isFinished = true;

		fileChannel.close();
		return new PartitionedFile(numSubpartitions, filePath, getFileIndex());
	}

	/**
	 * Used to close and delete the failed {@link PartitionedFile} when any exception occurs.
	 */
	public void releaseQuietly() {
		IOUtils.closeQuietly(fileChannel);
		IOUtils.deleteFileQuietly(filePath);
	}

	private PartitionedFile.PartitionedFileIndex getFileIndex() {
		long sumBytes = 0;
		for (int i = 0; i < numSubpartitions; ++i) {
			long numBytes = numSubpartitionBytes[i];
			numSubpartitionBytes[i] = sumBytes;
			sumBytes += numBytes;
		}

		return new PartitionedFile.PartitionedFileIndex(numSubpartitionBuffers, numSubpartitionBytes);
	}
}
