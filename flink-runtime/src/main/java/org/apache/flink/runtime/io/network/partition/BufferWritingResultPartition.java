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
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link ResultPartition} that turns records into buffers and writes directly to subpartitions.
 * This is in contrast to implementations where records are written to a joint structure, from
 * which the subpartitions draw the data after the write phase is finished, for example sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all partitions return buffers (and backlog) to be
 * transported through the network.
 */
public class BufferWritingResultPartition extends ResultPartition {

	private final BufferBuilder[] subpartitionBufferBuilders;

	@Nullable
	private BufferBuilder broadcastBufferBuilder;

	private final BufferWritingSubpartition[] bufferWritingSubpartitions;

	private Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

	public BufferWritingResultPartition(
			String owningTaskName,
			int partitionIndex,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			BufferWritingSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			@Nullable BufferCompressor bufferCompressor,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			partitionType,
			subpartitions,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.bufferWritingSubpartitions = subpartitions;
		this.subpartitionBufferBuilders = new BufferBuilder[subpartitions.length];
	}

	@Override
	public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getBufferBuilderForSinglePartitionEmit(targetSubpartition);
			bufferBuilder.appendAndCommit(record);

			if (bufferBuilder.isFull()) {
				finishSubpartitionBufferBuilder(targetSubpartition);
			}
		}
		while (record.hasRemaining());
	}

	@Override
	public void emitRecordToAllSubpartitions(ByteBuffer record) throws IOException {
		for (int subpartitionIndex = 0; subpartitionIndex < subpartitionBufferBuilders.length; subpartitionIndex++) {
			record.rewind();
			emitRecord(record, subpartitionIndex);
		}
	}

	@Override
	public void broadcastRecord(ByteBuffer record) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getBufferBuilderForBroadcast();
			bufferBuilder.appendAndCommit(record);

			if (bufferBuilder.isFull()) {
				finishBroadcastBufferBuilder();
			}
		}
		while (record.hasRemaining());
	}

	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		super.setMetricGroup(metrics);
		idleTimeMsPerSecond = metrics.getIdleTimeMsPerSecond();
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int subpartitionIndex = 0; subpartitionIndex < subpartitionBufferBuilders.length; subpartitionIndex++) {
				finishSubpartitionBufferBuilder(subpartitionIndex);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				addBufferConsumer(eventBufferConsumer.copy(), subpartitionIndex, isPriorityEvent);
			}
		}
	}

	@Override
	public void finish() throws IOException {
		finishBroadcastBufferBuilder();
		finishAllSubpartitionBufferBuilders();
		super.finish();
	}

	// ------------------------------------------------------------------------

	private BufferBuilder getBufferBuilderForSinglePartitionEmit(int targetPartition) throws IOException {
		final BufferBuilder current = subpartitionBufferBuilders[targetPartition];
		if (current != null) {
			return current;
		}

		return getNewBufferBuilderForSingleSubpartitionEmit(targetPartition);
	}

	private BufferBuilder getNewBufferBuilderForSingleSubpartitionEmit(int targetSubpartition) throws IOException {
		checkInProduceState();
		ensureUnicastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
		subpartitionBufferBuilders[targetSubpartition] = bufferBuilder;
		addBufferConsumer(bufferBuilder.createBufferConsumer(), targetSubpartition, false);
		return bufferBuilder;
	}

	private BufferBuilder getBufferBuilderForBroadcast() throws IOException {
		if (broadcastBufferBuilder != null) {
			return broadcastBufferBuilder;
		}

		return getNewBufferBuilderForBroadcast();
	}

	private BufferBuilder getNewBufferBuilderForBroadcast() throws IOException {
		checkInProduceState();
		ensureBroadcastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
		broadcastBufferBuilder = bufferBuilder;

		try (final BufferConsumer consumer = bufferBuilder.createBufferConsumer()) {
			for (int subpartitionIndex = 0; subpartitionIndex < subpartitionBufferBuilders.length; subpartitionIndex++) {
				addBufferConsumer(consumer.copy(), subpartitionIndex, false);
			}
		}

		return bufferBuilder;
	}

	private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition) throws IOException {
		BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
		if (bufferBuilder != null) {
			return bufferBuilder;
		}

		final long start = System.currentTimeMillis();
		try {
			bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
			idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
			return bufferBuilder;
		} catch (InterruptedException e) {
			throw new IOException("Interrupted while waiting for buffer");
		}
	}

	private void finishSubpartitionBufferBuilder(int targetSubpartition) {
		final BufferBuilder bufferBuilder = subpartitionBufferBuilders[targetSubpartition];
		if (bufferBuilder != null) {
			numBytesOut.inc(bufferBuilder.finish());
			numBuffersOut.inc();
			subpartitionBufferBuilders[targetSubpartition] = null;
		}
	}

	private void finishAllSubpartitionBufferBuilders() {
		for (int subpartitionIndex = 0; subpartitionIndex < subpartitionBufferBuilders.length; subpartitionIndex++) {
			finishSubpartitionBufferBuilder(subpartitionIndex);
		}
	}

	private void finishBroadcastBufferBuilder() {
		if (broadcastBufferBuilder != null) {
			numBytesOut.inc(broadcastBufferBuilder.finish() * subpartitionBufferBuilders.length);
			numBuffersOut.inc(subpartitionBufferBuilders.length);
			broadcastBufferBuilder = null;
		}
	}

	private void ensureUnicastMode() {
		finishBroadcastBufferBuilder();
	}

	private void ensureBroadcastMode() {
		finishAllSubpartitionBufferBuilders();
	}

	@VisibleForTesting
	public void addBufferConsumer(
			BufferConsumer bufferConsumer,
			int targetSubpartition,
			boolean isPriorityEvent) throws IOException {
		bufferWritingSubpartitions[targetSubpartition].add(bufferConsumer, isPriorityEvent);
	}

	@VisibleForTesting
	public Meter getIdleTimeMsPerSecond() {
		return idleTimeMsPerSecond;
	}
}
