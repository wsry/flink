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
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.runtime.io.network.partition.SortBuffer.BufferWithChannel;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SortMergeResultPartition} appends records and events to {@link SortBuffer} and after the
 * {@link SortBuffer} is full, all data in the {@link SortBuffer} will be copied and spilled to a
 * {@link PartitionedFile} in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link SortBuffer} will be spilled to the result {@link PartitionedFile}
 * separately.
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

	/** Number of data buffers (excluding events) written for each subpartition. */
	private final int[] numDataBuffers;

	private final int[] numDataAndEventBuffers;

	/** A piece of unmanaged memory for data writing. */
	private final MemorySegment writeBuffer;

	/** Size of network buffer and write buffer. */
	private final int networkBufferSize;

	/** File writer for this result partition. */
	private final PartitionedFileWriter fileWriter;

	/** {@link SortBuffer} for records to be sent to a single subpartition. */
	private SortBuffer unicastSortBuffer;

	/** {@link SortBuffer} for records to be sent to all subpartitions. */
	private SortBuffer broadcastSortBuffer;

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
		this.numDataBuffers = new int[numSubpartitions];
		this.numDataAndEventBuffers = new int[numSubpartitions];
		this.writeBuffer = MemorySegmentFactory.allocateUnpooledOffHeapMemory(networkBufferSize);

		PartitionedFileWriter fileWriter = null;
		try {
			// allocate at most 4M direct memory for caching of index entries
			fileWriter = new PartitionedFileWriter(numSubpartitions, 4194304, resultFileBasePath);
		} catch (Throwable throwable) {
			ExceptionUtils.rethrow(throwable);
		}
		this.fileWriter = fileWriter;
	}

	@Override
	protected void releaseInternal() {
		synchronized (lock) {
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
		emit(record, targetSubpartition, DataType.DATA_BUFFER, false);
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
		emit(record, 0, dataType, true);
	}

	private void emit(
			ByteBuffer record,
			int targetSubpartition,
			DataType dataType,
			boolean isBroadcast) throws IOException {
		checkInProduceState();

		SortBuffer sortBuffer = isBroadcast ? getBroadcastSortBuffer() : getUnicastSortBuffer();
		if (sortBuffer.append(record, targetSubpartition, dataType)) {
			return;
		}

		if (!sortBuffer.hasRemaining()) {
			// the record can not be appended to the free sort buffer because it is too large
			sortBuffer.finish();
			sortBuffer.release();
			writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
			return;
		}

		flushSortBuffer(sortBuffer, isBroadcast);
		emit(record, targetSubpartition, dataType, isBroadcast);
	}

	private void releaseSortBuffer(SortBuffer sortBuffer) {
		if (sortBuffer != null) {
			sortBuffer.release();
		}
	}

	private SortBuffer getUnicastSortBuffer() throws IOException {
		flushSortBuffer(broadcastSortBuffer, true);

		if (unicastSortBuffer != null && !unicastSortBuffer.isFinished()) {
			return unicastSortBuffer;
		}

		unicastSortBuffer = new PartitionSortedBuffer(
			lock, bufferPool, numSubpartitions, networkBufferSize, null);
		return unicastSortBuffer;
	}

	private SortBuffer getBroadcastSortBuffer() throws IOException {
		flushSortBuffer(unicastSortBuffer, false);

		if (broadcastSortBuffer != null && !broadcastSortBuffer.isFinished()) {
			return broadcastSortBuffer;
		}

		broadcastSortBuffer = new PartitionSortedBuffer(
			lock, bufferPool, numSubpartitions, networkBufferSize, null);
		return broadcastSortBuffer;
	}

	private void flushSortBuffer(SortBuffer sortBuffer, boolean isBroadcast) throws IOException {
		if (sortBuffer == null || sortBuffer.isReleased()) {
			return;
		}
		sortBuffer.finish();

		if (sortBuffer.hasRemaining()) {
			fileWriter.startNewRegion(isBroadcast);

			while (sortBuffer.hasRemaining()) {
				BufferWithChannel bufferWithChannel = sortBuffer.copyIntoSegment(writeBuffer);
				Buffer buffer = bufferWithChannel.getBuffer();
				int subpartitionIndex = bufferWithChannel.getChannelIndex();

				writeCompressedBufferIfPossible(buffer, subpartitionIndex, isBroadcast);
			}
		}

		releaseSortBuffer(sortBuffer);
	}

	private void writeCompressedBufferIfPossible(
			Buffer buffer,
			int targetSubpartition,
			boolean isBroadcast) throws IOException {
		updateStatistics(buffer, targetSubpartition, isBroadcast);
		try {
			if (canBeCompressed(buffer)) {
				buffer = bufferCompressor.compressToIntermediateBuffer(buffer);
			}

			if (isBroadcast) {
				fileWriter.broadcastBuffer(buffer);
			} else {
				fileWriter.writeBuffer(buffer, targetSubpartition);
			}
		} finally {
			buffer.recycleBuffer();
		}
	}

	private void updateStatistics(Buffer buffer, int subpartitionIndex, boolean isBroadcast) {
		numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
		long readableBytes = buffer.readableBytes();
		numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);

		if (!isBroadcast) {
			++numDataAndEventBuffers[subpartitionIndex];
			if (buffer.isBuffer()) {
				++numDataBuffers[subpartitionIndex];
			}
		} else {
			for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
				++numDataAndEventBuffers[subpartition];
				if (buffer.isBuffer()) {
					++numDataBuffers[subpartition];
				}
			}
		}
	}

	/**
	 * Spills the large record into the target {@link PartitionedFile} as a separate data region.
	 */
	private void writeLargeRecord(
			ByteBuffer record,
			int targetSubpartition,
			DataType dataType,
			boolean isBroadcast) throws IOException {
		fileWriter.startNewRegion(isBroadcast);

		while (record.hasRemaining()) {
			int toCopy = Math.min(record.remaining(), writeBuffer.size());
			writeBuffer.put(0, record, toCopy);
			NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);

			writeCompressedBufferIfPossible(buffer, targetSubpartition, isBroadcast);
		}
	}

	void releaseReader(SortMergeSubpartitionReader reader) {
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
		broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
		checkState(unicastSortBuffer == null || unicastSortBuffer.isReleased(),
			"The unicast sort buffer should be either null or released.");
		flushSortBuffer(broadcastSortBuffer, true);

		synchronized (lock) {
			checkState(!isReleased(), "Result partition is already released.");

			resultFile = fileWriter.finish();
			LOG.info("New partitioned file produced: {}.", resultFile);
		}

		super.finish();
	}

	@Override
	public void close() {
		releaseSortBuffer(unicastSortBuffer);
		releaseSortBuffer(broadcastSortBuffer);
		super.close();

		IOUtils.closeQuietly(fileWriter);
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (lock) {
			checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
			checkState(!isReleased(), "Partition released.");
			checkState(isFinished(), "Trying to read unfinished blocking partition.");

			SortMergeSubpartitionReader reader = new SortMergeSubpartitionReader(
				subpartitionIndex,
				numDataBuffers[subpartitionIndex],
				numDataAndEventBuffers[subpartitionIndex],
				this,
				availabilityListener,
				resultFile);
			readers.add(reader);

			return reader;
		}
	}

	private void flushInternal() throws IOException {
		if (unicastSortBuffer != null && !unicastSortBuffer.isReleased()) {
			checkState(broadcastSortBuffer == null || broadcastSortBuffer.isReleased(),
				"The broadcast sort buffer should be either null or released.");
			flushSortBuffer(unicastSortBuffer, false);
		}

		if (broadcastSortBuffer != null && !broadcastSortBuffer.isReleased()) {
			checkState(unicastSortBuffer == null || unicastSortBuffer.isReleased(),
				"The unicast sort buffer should be either null or released.");
			flushSortBuffer(broadcastSortBuffer, true);
		}
	}

	@Override
	public void flushAll() {
		try {
			flushInternal();
		} catch (IOException e) {
			LOG.error("Failed to flush the current sort buffer.", e);
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		try {
			flushInternal();
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
