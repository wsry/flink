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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.runtime.io.network.partition.SortBuffer.BufferWithChannel;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} implementation which appends records and events to {@link SortBuffer} and
 * after the {@link SortBuffer} is full, all data in the {@link SortBuffer} will be copied and spilled
 * to a {@link PartitionedFile} in subpartition index order. Each large record that can not be appended
 * to an empty {@link SortBuffer} will be spilled to a separate {@link PartitionedFile}.
 */
@NotThreadSafe
public class SortMergeResultPartition extends ResultPartition {

	private final Object lock = new Object();

	/** All active readers which are consuming data from this result partition now. */
	@GuardedBy("lock")
	private final Set<SortMergePartitionReader> readers = new HashSet<>();

	/** All {@link PartitionedFile}s produced by this result partition. */
	@GuardedBy("lock")
	private final ArrayList<PartitionedFile> partitionedFiles = new ArrayList<>();

	/** Used to generate random file channel ID. */
	private final FileChannelManager channelManager;

	/** Number of data buffers (excluding events) written for each subpartition. */
	private final int[] numDataBuffers;

	/** A piece of unmanaged memory for data writing. */
	private final MemorySegment writeBuffer;

	/** Size of network buffer and write buffer. */
	private final int networkBufferSize;

	/** Current {@link SortBuffer} to append records to. Each spills one {@link PartitionedFile}. */
	private SortBuffer currentSortBuffer;

	public SortMergeResultPartition(
			String owningTaskName,
			int partitionIndex,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			int numSubpartitions,
			int numTargetKeyGroups,
			int networkBufferSize,
			ResultPartitionManager partitionManager,
			FileChannelManager channelManager,
			@Nullable BufferCompressor bufferCompressor,
			SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			partitionType,
			numSubpartitions,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.channelManager = checkNotNull(channelManager);
		this.networkBufferSize = networkBufferSize;
		this.numDataBuffers = new int[numSubpartitions];
		this.writeBuffer = MemorySegmentFactory.allocateUnpooledOffHeapMemory(networkBufferSize);
	}

	@Override
	protected void releaseInternal() {
		synchronized (lock) {
			isFinished = true; // to fail writing faster

			// delete the partitioned files only when no reader is reading now
			if (readers.isEmpty()) {
				for (PartitionedFile partitionedFile : partitionedFiles) {
					partitionedFile.deleteQuietly();
				}
				partitionedFiles.clear();
			}
		}
	}

	@Override
	public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
		emit(record, targetSubpartition, DataType.DATA_BUFFER);
	}

	@Override
	public void broadcastRecord(ByteBuffer record) throws IOException {
		broadcast(record, DataType.DATA_BUFFER);
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
		try {
			ByteBuffer serializedEvent = buffer.getNioBufferReadable();
			broadcast(serializedEvent, buffer.getDataType());
		} finally {
			buffer.recycleBuffer();
		}
	}

	private void broadcast(ByteBuffer record, DataType dataType) throws IOException {
		for (int channelIndex = 0; channelIndex < numSubpartitions; ++channelIndex) {
			record.rewind();
			emit(record, channelIndex, dataType);
		}
	}

	private void emit(ByteBuffer record, int targetSubpartition, DataType dataType) throws IOException {
		checkInProduceState();

		SortBuffer sortBuffer = getSortBuffer();
		if (sortBuffer.append(record, targetSubpartition, dataType)) {
			return;
		}

		if (!sortBuffer.hasRemaining()) {
			// the record can not be appended to the free sort buffer because it is too large
			releaseCurrentSortBuffer();
			writeLargeRecord(record, targetSubpartition, dataType);
			return;
		}

		flushCurrentSortBuffer();
		emit(record, targetSubpartition, dataType);
	}

	private void releaseCurrentSortBuffer() {
		if (currentSortBuffer != null) {
			currentSortBuffer.release();
			currentSortBuffer = null;
		}
	}

	private SortBuffer getSortBuffer() {
		if (currentSortBuffer != null) {
			return currentSortBuffer;
		}

		currentSortBuffer = new PartitionSortedBuffer(bufferPool, numSubpartitions, networkBufferSize);
		return currentSortBuffer;
	}

	private void flushCurrentSortBuffer() throws IOException {
		if (currentSortBuffer == null || !currentSortBuffer.hasRemaining()) {
			releaseCurrentSortBuffer();
			return;
		}

		currentSortBuffer.finish();
		String basePath = channelManager.createChannel().getPath();
		PartitionedFileWriter partitionedFileWriter = new PartitionedFileWriter(basePath, numSubpartitions);
		PartitionedFile partitionedFile;

		try {
			partitionedFileWriter.open();
			while (currentSortBuffer.hasRemaining()) {
				BufferWithChannel bufferWithChannel = currentSortBuffer.copyData(writeBuffer);
				Buffer buffer = bufferWithChannel.getBuffer();
				int subpartitionIndex = bufferWithChannel.getChannelIndex();

				Buffer compressedBuffer = compressIfPossible(buffer);
				partitionedFileWriter.writeBuffer(compressedBuffer, subpartitionIndex);
				compressedBuffer.recycleBuffer();

				updateStatistics(buffer, subpartitionIndex);
			}
			partitionedFile = partitionedFileWriter.finish();
		} catch (Throwable throwable) {
			// ensure that the failed partitioned file will be closed and deleted
			partitionedFileWriter.releaseQuietly();
			throw throwable;
		}

		addNewPartitionedFile(partitionedFile);
		releaseCurrentSortBuffer();
	}

	private Buffer compressIfPossible(Buffer buffer) {
		if (canBeCompressed(buffer)) {
			return bufferCompressor.compressToIntermediateBuffer(buffer);
		}
		return buffer;
	}

	private void updateStatistics(Buffer buffer, int subpartitionIndex) {
		numBuffersOut.inc();
		numBytesOut.inc(buffer.readableBytes());
		if (buffer.isBuffer()) {
			++numDataBuffers[subpartitionIndex];
		}
	}

	/**
	 * Spills the large record into a separate {@link PartitionedFile}.
	 */
	private void writeLargeRecord(ByteBuffer record, int targetSubpartition, DataType dataType) throws IOException {
		String basePath = channelManager.createChannel().getPath();
		PartitionedFileWriter partitionedFileWriter = new PartitionedFileWriter(basePath, numSubpartitions);
		PartitionedFile partitionedFile;

		try {
			partitionedFileWriter.open();
			while (record.hasRemaining()) {
				int toCopy = Math.min(record.remaining(), writeBuffer.size());
				writeBuffer.put(0, record, toCopy);

				NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
				partitionedFileWriter.writeBuffer(buffer, targetSubpartition);
			}
			partitionedFile = partitionedFileWriter.finish();
		} catch (Throwable throwable) {
			// ensure that the failed partitioned file will be closed and deleted
			partitionedFileWriter.releaseQuietly();
			throw throwable;
		}

		addNewPartitionedFile(partitionedFile);
	}

	private void addNewPartitionedFile(PartitionedFile partitionedFile) {
		synchronized (lock) {
			if (isReleased()) {
				partitionedFile.deleteQuietly();
				throw new IllegalStateException("Partition is already released.");
			}
			partitionedFiles.add(partitionedFile);
		}
	}

	void releaseReader(SortMergePartitionReader reader) {
		synchronized (lock) {
			readers.remove(reader);

			// release the result partition if it has been marked as released
			if (readers.isEmpty() && isReleased()) {
				releaseInternal();
			}
		}
	}

	@Override
	public void finish() throws IOException {
		checkInProduceState();

		broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
		flushCurrentSortBuffer();

		super.finish();
	}

	@Override
	public void close() {
		releaseCurrentSortBuffer();

		super.close();
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (lock) {
			checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
			checkState(!isReleased(), "Partition released.");
			checkState(isFinished(), "Trying to read unfinished blocking partition.");

			availabilityListener.notifyDataAvailable();

			Queue<PartitionedFile> files = new ArrayDeque<>(partitionedFiles);
			SortMergePartitionReader reader = new SortMergePartitionReader(
				this,
				files,
				availabilityListener,
				subpartitionIndex,
				numDataBuffers[subpartitionIndex],
				networkBufferSize);
			readers.add(reader);

			return reader;
		}
	}

	@Override
	public void flushAll() {
		try {
			flushCurrentSortBuffer();
		} catch (IOException e) {
			LOG.error("Failed to flush the current sort buffer.", e);
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		try {
			flushCurrentSortBuffer();
		} catch (IOException e) {
			LOG.error("Failed to flush the current sort buffer.", e);
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return AVAILABLE;
	}

	@Override
	public int getNumberOfQueuedBuffers() {
		return 0;
	}

	@Override
	public int getNumberOfQueuedBuffers(int targetSubpartition) {
		return 0;
	}

	@VisibleForTesting
	public int getNumPartitionedFiles() {
		return partitionedFiles.size();
	}
}
