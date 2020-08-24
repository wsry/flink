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

/**
 * A {@link ResultSubpartition} where buffers are written to directly.
 * This is in contrast to subpartition implementations where records are written to a different
 * structure, from which the subpartitions draw the data (like sort-based partitioning).
 *
 * <p>To avoid confusion: On the read side, all partitions return buffers (and backlog) to be
 * transported through the network ch
 */
public abstract class BufferWritingSubpartition extends ResultSubpartition {

	public BufferWritingSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}
}
