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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;

import java.io.IOException;

/**
 * A result of a task, pipelined (streamed) to the receivers.
 *
 * <p>This result partition implementation is used both in batch and streaming. For streaming it supports
 * low latency transfers (ensure data is sent within x milliseconds) or unconstrained while for batch it
 * transfers only once a buffer is full. Additionally, for streaming use this typically limits the length of the
 * buffer backlog to not have too much data in fligh, while for batch we do not constrain this.
 */
public class PipelinedResultPartition extends BufferOrientedResultPartition implements CheckpointedResultPartition {

	@Override
	public PipelinedSubpartition[] getAllPartitions() {
		return (PipelinedSubpartition[]) super.getAllPartitions();
	}

	@Override
	public void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException {
		for (PipelinedSubpartition subpartition : getAllPartitions()) {
			subpartition.readRecoveredState(stateReader);
		}
		LOG.debug("{}: Finished reading recovered state.", this);
	}

}
