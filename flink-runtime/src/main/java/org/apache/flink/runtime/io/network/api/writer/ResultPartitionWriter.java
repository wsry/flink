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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A record-oriented runtime result writer API for producing results.
 *
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link ResultPartitionWriter#fail(Throwable)} or
 * {@link ResultPartitionWriter#finish()}, it abruptly triggers failure and cancellation of production.
 * In this case {@link ResultPartitionWriter#fail(Throwable)} still needs to be called afterwards to fully release
 * all resources associated the the partition and propagate failure cause to the consumer if possible.
 */
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

	/**
	 * Setup partition, potentially heavy-weight, blocking operation comparing to just creation.
	 */
	void setup() throws IOException;

	ResultPartitionID getPartitionId();

	int getNumberOfSubpartitions();

	int getNumTargetKeyGroups();

	/**
	 * Writes the given serialized record to the target subpartition.
	 */
	void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

	/**
	 * Writes the given serialized record to all subpartitions.
	 */
	void emitRecordToAllSubpartitions(ByteBuffer record) throws IOException;

	/**
	 * Writes the given serialized record to a shared channel which can be consumed by all subpartitions.
	 *
	 * <p>Note: Different from {@link #emitRecordToAllSubpartitions(ByteBuffer)}, this method copies the
	 * given serialized record only once so can have better performance for continuous record broadcast.
	 * However, for non-frequent random broadcast, {@link #emitRecordToAllSubpartitions(ByteBuffer)} is
	 * better because this method can incur extra overhead.
	 */
	void broadcastRecord(ByteBuffer record) throws IOException;

	/**
	 * Writes the given {@link AbstractEvent} to all channels.
	 */
	void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

	/**
	 * Sets the metric group for the {@link ResultPartitionWriter}.
	 */
	void setMetricGroup(TaskIOMetricGroup metrics);

	/**
	 * Returns the subpartition with the given index.
	 */
	ResultSubpartition getSubpartition(int subpartitionIndex);

	/**
	 * Manually trigger the consumption of data from all subpartitions.
	 */
	void flushAll();

	/**
	 * Manually trigger the consumption of data from the given subpartitions.
	 */
	void flush(int subpartitionIndex);

	/**
	 * Fail the production of the partition.
	 *
	 * <p>This method propagates non-{@code null} failure causes to consumers on a best-effort basis. This call also
	 * leads to the release of all resources associated with the partition. Closing of the partition is still needed
	 * afterwards if it has not been done before.
	 *
	 * @param throwable failure cause
	 */
	void fail(@Nullable Throwable throwable);

	/**
	 * Successfully finish the production of the partition.
	 *
	 * <p>Closing of partition is still needed afterwards.
	 */
	void finish() throws IOException;
}
