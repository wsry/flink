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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.netty.ServerOutboundMessage.AddBacklogMessage;
import org.apache.flink.runtime.io.network.netty.ServerOutboundMessage.BufferResponseMessage;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final PartitionRequestQueue requestQueue;

	private final boolean withoutExclusiveCredits;

	private volatile ResultSubpartitionView subpartitionView;

	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private boolean isRegisteredAsAvailable = false;

	/** The number of available buffers for holding data on the consumer side. */
	private int numCreditsAvailable;

	private int sequenceNumber = -1;

	CreditBasedSequenceNumberingViewReader(
			InputChannelID receiverId,
			int initialCredit,
			PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
		this.withoutExclusiveCredits = initialCredit == 0;
	}

	@Override
	public void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(
					resultPartitionId,
					subPartitionIndex,
					this);
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int creditDeltas) throws Exception {
		numCreditsAvailable += creditDeltas;
		requestQueue.enqueueAvailableReader(this, this::isAvailable);
	}

	@Override
	public boolean shouldAnnounceBacklog(boolean hasUnfulfilledBacklog) {
		return !hasUnfulfilledBacklog && withoutExclusiveCredits && numCreditsAvailable == 0;
	}

	@Override
	public void resumeConsumption(int availableCredits, boolean hasUnfulfilledBacklog) throws Exception {
		// reset the available credit
		numCreditsAvailable = availableCredits;
		subpartitionView.resumeConsumption();
		requestQueue.enqueueAvailableReader(this, () -> (isAvailable() || shouldAnnounceBacklog(hasUnfulfilledBacklog)));
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		return subpartitionView.isAvailable(numCreditsAvailable);
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog
	 * 		current buffer and backlog including information about the next buffer
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		if (numCreditsAvailable > 0) {
			return bufferAndBacklog.isDataAvailable();
		}
		else {
			return bufferAndBacklog.isEventAvailable();
		}
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable(Integer.MAX_VALUE);
	}

	private AddBacklogMessage getAddBacklogMessage() {
		int backlog = subpartitionView.getAndResetUnannouncedBacklog();
		if (backlog > 0) {
			return new AddBacklogMessage(receiverId, backlog);
		}
		return null;
	}

	private BufferResponseMessage getBufferResponseMessage() throws IOException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			sequenceNumber++;

			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
				throw new IllegalStateException("no credit available");
			}

			return new BufferResponseMessage(
				next.buffer(), receiverId, sequenceNumber, next.unannouncedBacklog(), isAvailable(next));
		} else {
			return null;
		}
	}

	/**
	 * BufferResponse is processed with higher priority than AddBacklog. It is no need to send addBacklog
	 * message in below three scenarios:
	 * 1. It has exclusive credits to guarantee that the backlog can be always carried via BufferResponse.
	 * 2. It has available credits (no matter with exclusive or not) for now to guarantee backlog can be
	 *    carried by BufferResponse.
	 * 3. Next Buffer is an event which does not consume any credit.
	 */
	@Override
	public ServerOutboundMessage getNextMessage() throws IOException {
		if (!withoutExclusiveCredits || numCreditsAvailable > 0 || subpartitionView.isAvailable(0)) {
			return getBufferResponseMessage();
		}
		return getAddBacklogMessage();
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	@Override
	public void notifyDataAvailable() {
		requestQueue.notifyReaderNonEmpty(this);
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", withoutExclusiveCredits=" + withoutExclusiveCredits +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
