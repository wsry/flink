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
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.runtime.io.network.partition.SortBuffer.BufferWithChannel;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SortMergeResultPartition} appends records and events to {@link SortBuffer} and after the {@link SortBuffer}
 * is full, all data in the {@link SortBuffer} will be copied and spilled to a {@link PartitionedFile} in the random
 * subpartition index order sequentially. Large records that can not be appended to an empty {@link SortBuffer} will
 * be spilled to the {@link PartitionedFile} separately.
 */
@NotThreadSafe
public class SortMergeResultPartition extends ResultPartition {

	private final Object lock = new Object();

	/** All active readers which are consuming data from this result partition now. */
	@GuardedBy("lock")
	private final Set<SortMergeSubpartitionReader> readers = new HashSet<>();

	/** {@link PartitionedFile} produced by this result partition. */
	@GuardedBy("lock")
	private PartitionedFile resultFile;

	/** A piece of unmanaged memory for data writing. */
	private final MemorySegment writeBuffer;

	/** Size of network buffer and write buffer. */
	private final int networkBufferSize;

	/** File writer for this result partition. */
	private final PartitionedFileWriter fileWriter;

	/** Subpartition order of coping data from {@link SortBuffer}. */
	private final int[] subpartitionReadOrder;

	/** IO scheduler which controls shuffle data reading. */
	private final BlockingShuffleIOScheduler shuffleIOScheduler;

	/** Current {@link SortBuffer} to append records to. */
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
			String resultFileBasePath,
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

		this.networkBufferSize = networkBufferSize;
		this.writeBuffer = MemorySegmentFactory.allocateUnpooledOffHeapMemory(networkBufferSize);
		this.subpartitionReadOrder = getRandomSubpartitionReadOrder(numSubpartitions);
		this.fileWriter = createPartitionedFileWriter(resultFileBasePath, numSubpartitions);
		this.shuffleIOScheduler = createShuffleIOScheduler(networkBufferSize);
	}

	private int[] getRandomSubpartitionReadOrder(int numSubpartitions) {
		int[] subpartitionReadOrder = new int[numSubpartitions];
		ArrayList<Integer> tmpOrder = new ArrayList<>(numSubpartitions);

		for (int channel = 0; channel < numSubpartitions; ++channel) {
			tmpOrder.add(channel);
		}
		Collections.shuffle(tmpOrder);

		for (int channel = 0; channel < numSubpartitions; ++channel) {
			subpartitionReadOrder[channel] = tmpOrder.get(channel);
		}

		return subpartitionReadOrder;
	}

	private BlockingShuffleIOScheduler createShuffleIOScheduler(int networkBufferSize) {
		// the scheduler thread will be released when the result partition is released
		BlockingShuffleIOScheduler shuffleIOScheduler = new BlockingShuffleIOScheduler(networkBufferSize, lock);
		shuffleIOScheduler.start();

		return shuffleIOScheduler;
	}

	private PartitionedFileWriter createPartitionedFileWriter(String resultFileBasePath, int numSubpartitions) {
		// allocate about 4m heap memory for caching of index entries
		PartitionedFileWriter fileWriter = new PartitionedFileWriter(resultFileBasePath, numSubpartitions, 400 * 1024);

		try {
			fileWriter.open();
		} catch (Throwable throwable) {
			// ensure that the file writer is released when any exception occurs
			fileWriter.releaseQuietly();
			ExceptionUtils.rethrow(throwable);
		}

		return fileWriter;
	}

	@Override
	protected void releaseInternal() {
		synchronized (lock) {
			shuffleIOScheduler.release(getFailureCause());

			if (resultFile == null) {
				fileWriter.releaseQuietly();
			}

			// delete the produced file only when no reader is reading now
			if (readers.isEmpty()) {
				if (resultFile != null) {
					resultFile.deleteQuietly();
					resultFile = null;
				}
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

		currentSortBuffer = new PartitionSortedBuffer(
			bufferPool, numSubpartitions, networkBufferSize, lock, subpartitionReadOrder);
		return currentSortBuffer;
	}

	private void flushCurrentSortBuffer() throws IOException {
		if (currentSortBuffer == null) {
			return;
		}

		if (currentSortBuffer.hasRemaining()) {
			currentSortBuffer.finish();
			fileWriter.startNewRegion();

			while (currentSortBuffer.hasRemaining()) {
				BufferWithChannel bufferWithChannel = currentSortBuffer.copyIntoSegment(writeBuffer);
				Buffer buffer = bufferWithChannel.getBuffer();
				int subpartitionIndex = bufferWithChannel.getChannelIndex();

				writeCompressedBufferIfPossible(buffer, subpartitionIndex);
			}
		}

		releaseCurrentSortBuffer();
	}

	private void writeCompressedBufferIfPossible(Buffer buffer, int targetSubpartition) throws IOException {
		updateStatistics(buffer);

		try {
			if (canBeCompressed(buffer)) {
				buffer = bufferCompressor.compressToIntermediateBuffer(buffer);
			}
			fileWriter.writeBuffer(buffer, targetSubpartition);
		} finally {
			buffer.recycleBuffer();
		}
	}

	private void updateStatistics(Buffer buffer) {
		numBuffersOut.inc();
		numBytesOut.inc(buffer.readableBytes());
	}

	/**
	 * Spills the large record into the target {@link PartitionedFile} as a separate data region.
	 */
	private void writeLargeRecord(ByteBuffer record, int targetSubpartition, DataType dataType) throws IOException {
		fileWriter.startNewRegion();

		while (record.hasRemaining()) {
			int toCopy = Math.min(record.remaining(), writeBuffer.size());
			writeBuffer.put(0, record, toCopy);
			NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);

			writeCompressedBufferIfPossible(buffer, targetSubpartition);
		}
	}

	void releaseReader(SortMergeSubpartitionReader reader) {
		synchronized (lock) {
			shuffleIOScheduler.releaseSubpartitionReader(reader);
			readers.remove(reader);

			// release the result partition if it has been marked as released
			if (readers.isEmpty() && isReleased()) {
				if (resultFile != null) {
					resultFile.deleteQuietly();
					resultFile = null;
				}
			}
		}
	}

	@Override
	public void finish() throws IOException {
		broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
		flushCurrentSortBuffer();

		synchronized (lock) {
			checkState(!isReleased(), "Result partition is already released.");

			resultFile = fileWriter.finish();
			LOG.info("New partitioned file produced: {}.", resultFile);
		}

		super.finish();
	}

	@Override
	public void close() {
		synchronized (lock) {
			releaseCurrentSortBuffer();
			// to fail writing faster
			fileWriter.close();

			super.close();
		}
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (lock) {
			checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
			checkState(!isReleased(), "Partition released.");
			checkState(isFinished(), "Trying to read unfinished blocking partition.");

			SortMergeSubpartitionReader reader = shuffleIOScheduler.crateSubpartitionReader(
				this,
				resultFile,
				availabilityListener,
				subpartitionIndex);
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
	PartitionedFile getResultFile() {
		return resultFile;
	}
}
