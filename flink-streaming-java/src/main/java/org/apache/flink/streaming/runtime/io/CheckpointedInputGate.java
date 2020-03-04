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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnblockCheckpointListener;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link CheckpointedInputGate} uses {@link CheckpointBarrierHandler} to handle incoming
 * {@link CheckpointBarrier} from the {@link InputGate}.
 */
@Internal
public class CheckpointedInputGate implements PullingAsyncDataInput<BufferOrEvent>, UnblockCheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointedInputGate.class);

	private final CheckpointBarrierHandler barrierHandler;

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	private final int channelIndexOffset;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfInputGate;

	/** Indicate end of the input. Set to true after encountering {@link #endOfInputGate}. */
	private boolean isFinished;

	public CheckpointedInputGate(
			InputGate inputGate,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		this(
			inputGate,
			new CheckpointBarrierAligner(
				inputGate.getNumberOfInputChannels(),
				taskName,
				toNotifyOnCheckpoint)
		);
	}

	public CheckpointedInputGate(
			InputGate inputGate,
			CheckpointBarrierHandler barrierHandler) {
		this(inputGate, barrierHandler, 0);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param barrierHandler Handler that controls which channels are blocked.
	 * @param channelIndexOffset Optional offset added to channelIndex returned from the inputGate
	 *                           before passing it to the barrierHandler.
	 */
	public CheckpointedInputGate(
			InputGate inputGate,
			CheckpointBarrierHandler barrierHandler,
			int channelIndexOffset) {
		this.inputGate = inputGate;
		this.channelIndexOffset = channelIndexOffset;
		this.barrierHandler = barrierHandler;
		this.barrierHandler.registerUnblockCheckpointListener(this);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return inputGate.getAvailableFuture();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			Optional<BufferOrEvent> next = inputGate.pollNext();

			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (barrierHandler.isBlocked(offsetChannelIndex(bufferOrEvent.getChannelIndex()))) {
				throw new IllegalStateException("Channel already blocked.");
			}
			else if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
				if (!endOfInputGate) {
					// process barriers only if there is a chance of the checkpoint completing
					barrierHandler.processBarrier(checkpointBarrier, this, offsetChannelIndex(bufferOrEvent.getChannelIndex()));
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
			}
			else {
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					barrierHandler.processEndOfPartition();
				}
				return next;
			}
		}
	}

	private int offsetChannelIndex(int channelIndex) {
		return channelIndex + channelIndexOffset;
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfInputGate) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfInputGate = true;
			barrierHandler.releaseBlocksAndResetBarriers();
			return pollNext();
		}
	}

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return true;
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	public void cleanup() throws IOException {
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getLatestCheckpointId() {
		return barrierHandler.getLatestCheckpointId();
	}

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	@VisibleForTesting
	long getAlignmentDurationNanos() {
		return barrierHandler.getAlignmentDurationNanos();
	}

	/**
	 * @return the time that elapsed, in nanoseconds, between the creation of the latest checkpoint
	 * and the time when it's first {@link CheckpointBarrier} was received by this {@link InputGate}.
	 */
	@VisibleForTesting
	long getCheckpointStartDelayNanos() {
		return barrierHandler.getCheckpointStartDelayNanos();
	}

	/**
	 * @return number of underlying input channels.
	 */
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return barrierHandler.toString();
	}

	@Override
	public void unblockChannel(long checkpointId, int channelIndex) throws Exception {
		inputGate.unblockChannel(checkpointId, channelIndex - channelIndexOffset);
	}

	@Override
	public void unblockAllChannels(long checkpointId) throws Exception {
		inputGate.unblockAllChannels(checkpointId);
	}

	@Override
	public void unblockAllChannelsExceptFor(long checkpointId, int channelIndex) throws Exception {
		inputGate.unblockAllChannelsExceptFor(checkpointId, channelIndex - channelIndexOffset);
	}
}
