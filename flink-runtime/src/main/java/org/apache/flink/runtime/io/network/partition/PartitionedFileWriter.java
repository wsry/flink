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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File writer which can write buffers and generate the {@link PartitionedFile}. Data is written region by region.
 * Before writing any data, {@link #open} must be called and before writing a new region, {@link #startNewRegion}
 * must be called. After writing all data, {@link #finish} must be called to close all opened files and return the
 * target {@link PartitionedFile}.
 */
@NotThreadSafe
public class PartitionedFileWriter implements AutoCloseable {

	/** Buffer for writing region index. */
	private final ByteBuffer indexBuffer;

	/** Number of channels. When writing a buffer, target subpartition must be in this range. */
	private final int numSubpartitions;

	/** Data file path of the target {@link PartitionedFile}. */
	private final Path dataFilePath;

	/** Index file path of the target {@link PartitionedFile}. */
	private final Path indexFilePath;

	/** Offset in the data file for each subpartition in the current region. */
	private final long[] subpartitionOffsets;

	/** Number of buffers written for each subpartition in the current region. */
	private final int[] subpartitionBuffers;

	/** Used to cache data before writing to disk for better read performance. */
	private ByteBuffer writeDataCache;

	/** Opened data file channel of the target {@link PartitionedFile}. */
	private FileChannel dataFileChannel;

	/** Opened index file channel of the target {@link PartitionedFile}. */
	private FileChannel indexFileChannel;

	/** Number of bytes written to the target {@link PartitionedFile}. */
	private long totalBytesWritten;

	/** Number of regions written to the target {@link PartitionedFile}. */
	private int numRegions;

	/** Current subpartition to write. Buffer writing must be in subpartition order within each region. */
	private int currentSubpartition = -1;

	/** Whether all index data is cached in memory or not. */
	private boolean allIndexDataCached = true;

	/** Whether this file writer is finished or not. */
	private boolean isFinished;

	/** Whether this file writer is closed or not. */
	private boolean isClosed;

	public PartitionedFileWriter(String basePath, int numSubpartitions, int indexBufferSize) {
		checkArgument(basePath != null, "Base path must not be null.");
		checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");
		checkArgument(indexBufferSize > 0, "Illegal index buffer size.");

		this.numSubpartitions = numSubpartitions;
		this.subpartitionOffsets = new long[numSubpartitions];
		this.subpartitionBuffers = new int[numSubpartitions];
		this.dataFilePath = new File(basePath + PartitionedFile.DATA_FILE_SUFFIX).toPath();
		this.indexFilePath = new File(basePath + PartitionedFile.INDEX_FILE_SUFFIX).toPath();

		this.indexBuffer = ByteBuffer.allocate(indexBufferSize * PartitionedFile.INDEX_ENTRY_SIZE);
		indexBuffer.order(PartitionedFile.DEFAULT_BYTE_ORDER);

		Arrays.fill(subpartitionOffsets, -1L);

		// allocate 4M unmanaged direct memory for caching of data
		this.writeDataCache = ByteBuffer.allocateDirect(4 * 1024 * 1024);
		BufferReaderWriterUtil.configureByteBuffer(writeDataCache);
	}

	/**
	 * Opens the {@link PartitionedFile} for writing.
	 *
	 * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any exception
	 * occurs.
	 */
	public void open() throws IOException {
		checkState(!isFinished, "File writer is already finished.");
		checkState(!isClosed, "File writer is already closed.");
		checkState(dataFileChannel == null && indexFileChannel == null, "Partitioned file is already opened.");

		dataFileChannel = openFileChannel(dataFilePath);
		indexFileChannel = openFileChannel(indexFilePath);
	}

	private FileChannel openFileChannel(Path path) throws IOException {
		return FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
	}

	/**
	 * Persists the region index of the current data region and starts a new region to write.
	 *
	 * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any exception
	 * occurs.
	 */
	public void startNewRegion() throws IOException {
		checkState(!isFinished, "File writer is already finished.");
		checkState(!isClosed, "File writer is already closed.");
		checkState(dataFileChannel != null && indexFileChannel != null, "Must open the partitioned file first.");

		writeRegionIndex();
	}

	private void writeRegionIndex() throws IOException {
		if (Arrays.stream(subpartitionBuffers).sum() > 0) {
			for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
				if (!indexBuffer.hasRemaining()) {
					flushIndexBuffer();
					indexBuffer.clear();
					allIndexDataCached = false;
				}

				indexBuffer.putLong(subpartitionOffsets[subpartition]);
				indexBuffer.putInt(subpartitionBuffers[subpartition]);
			}

			currentSubpartition = -1;
			++numRegions;
			Arrays.fill(subpartitionOffsets, -1L);
			Arrays.fill(subpartitionBuffers, 0);
		}
	}

	private void flushIndexBuffer() throws IOException {
		indexBuffer.flip();
		if (indexBuffer.hasRemaining()) {
			BufferReaderWriterUtil.writeBuffer(indexFileChannel, indexBuffer);
		}
	}

	/**
	 * Writes a {@link Buffer} of the given subpartition to the this {@link PartitionedFile}. In a data region,
	 * all data of the same subpartition must be written together.
	 *
	 * <p>Note: The caller is responsible for recycling the target buffer and releasing the failed
	 * {@link PartitionedFile} if any exception occurs.
	 */
	public void writeBuffer(Buffer target, int targetSubpartition) throws IOException {
		checkState(!isFinished, "File writer is already finished.");
		checkState(!isClosed, "File writer is already closed.");
		checkState(dataFileChannel != null && indexFileChannel != null, "Must open the partitioned file first.");

		if (targetSubpartition != currentSubpartition) {
			checkState(subpartitionOffsets[targetSubpartition] == -1, "Must write data of the same channel together.");
			subpartitionOffsets[targetSubpartition] = totalBytesWritten;
			currentSubpartition = targetSubpartition;
		}

		totalBytesWritten += BufferReaderWriterUtil.writeToByteChannel(dataFileChannel, target, writeDataCache);
		++subpartitionBuffers[targetSubpartition];
	}

	/**
	 * Finishes writing which closes the file channel and returns the corresponding {@link PartitionedFile}.
	 *
	 * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any exception
	 * occurs.
	 */
	public PartitionedFile finish() throws IOException {
		checkState(!isFinished, "File writer is already finished.");
		checkState(!isClosed, "File writer is already closed.");
		checkState(dataFileChannel != null && indexFileChannel != null, "Must open the partitioned file first.");

		isFinished = true;

		writeDataCache.flip();
		if (writeDataCache.hasRemaining()) {
			BufferReaderWriterUtil.writeBuffer(dataFileChannel, writeDataCache);
		}
		writeDataCache = null;

		writeRegionIndex();
		flushIndexBuffer();
		indexBuffer.rewind();

		close();
		ByteBuffer indexDataCache = allIndexDataCached ? indexBuffer : null;
		return new PartitionedFile(numRegions, numSubpartitions, dataFilePath, indexFilePath, indexDataCache);
	}

	/**
	 * Used to close and delete the failed {@link PartitionedFile} when any exception occurs.
	 */
	public void releaseQuietly() {
		IOUtils.closeQuietly(this);
		IOUtils.deleteFileQuietly(dataFilePath);
		IOUtils.deleteFileQuietly(indexFilePath);
	}

	@Override
	public void close() {
		if (isClosed) {
			return;
		}
		isClosed = true;

		Throwable exception = null;
		try {
			dataFileChannel.close();
		} catch (Throwable throwable) {
			exception = throwable;
		}

		try {
			indexFileChannel.close();
		} catch (Throwable throwable) {
			exception = ExceptionUtils.firstOrSuppressed(throwable, exception);
		}

		if (exception != null) {
			ExceptionUtils.rethrow(exception);
		}
	}
}
