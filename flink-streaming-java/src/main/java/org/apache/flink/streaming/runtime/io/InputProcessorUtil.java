/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.util.Arrays;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode
 * for {@link StreamOneInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			InputGate inputGate,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName) {
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode, inputGate.getNumberOfInputChannels(), taskName, toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		return new CheckpointedInputGate(inputGate, barrierHandler);
	}

	/**
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedInputGatePair(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName,
			InputGate ...inputGates) {
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode,
			Arrays.stream(inputGates).mapToInt(InputGate::getNumberOfInputChannels).sum(),
			taskName,
			toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		CheckpointedInputGate[] checkpointedInputGates = new CheckpointedInputGate[inputGates.length];

		int channelIndexOffset = 0;
		for (int i = 0; i < inputGates.length; i++) {
			checkpointedInputGates[i] = new CheckpointedInputGate(inputGates[i], barrierHandler, channelIndexOffset);
			channelIndexOffset += inputGates[i].getNumberOfInputChannels();
		}

		return checkpointedInputGates;
	}

	private static CheckpointBarrierHandler createCheckpointBarrierHandler(
			CheckpointingMode checkpointMode,
			int numberOfInputChannels,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		switch (checkpointMode) {
			case EXACTLY_ONCE:
				return new CheckpointBarrierAligner(
					numberOfInputChannels,
					taskName,
					toNotifyOnCheckpoint);
			case AT_LEAST_ONCE:
				return new CheckpointBarrierTracker(numberOfInputChannels, toNotifyOnCheckpoint);
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
	}

	private static void registerCheckpointMetrics(TaskIOMetricGroup taskIOMetricGroup, CheckpointBarrierHandler barrierHandler) {
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_ALIGNMENT_TIME, barrierHandler::getAlignmentDurationNanos);
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_START_DELAY_TIME, barrierHandler::getCheckpointStartDelayNanos);
	}
}
