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

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link ResultPartition} that turns records into buffers and write directly to partitions.
 * This is in contrast to implementations where records are written to a joint
 * structure, from which the subpartitions draw the data after the write phase is finished,
 * for example sort-based partitioning.
 *
 * <p>To avoid confusion: On the read side, all partitions return buffers (and backlog) to be
 * transported through the network ch
 */
public class BufferWritingResultPartition extends ResultPartition {

	private final BufferWritingSubpartition[] bufferWritingSubpartitions;

	public BufferWritingResultPartition(
			String owningTaskName,
			int partitionIndex,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			BufferWritingSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			@Nullable BufferCompressor bufferCompressor,
			FunctionWithException<BufferPoolOwner,
			BufferPool, IOException> bufferPoolFactory) {

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
	}
}
