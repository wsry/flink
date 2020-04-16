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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer, boolean)} adds a finished {@link BufferConsumer} or a second
 * {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via
 * {@link ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except by calling
 * {@link #flush()} explicitly, we always only notify when the first finished buffer turns up and
 * then, the reader has to drain the buffers via {@link #pollBuffer()} until its return value shows
 * no more buffers being available. This results in a buffer queue which is either empty or has an
 * unfinished {@link BufferConsumer} left from which the notifications will eventually start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notification} for any
 * {@link BufferConsumer} present in the queue.
 */
public class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<BufferConsumer> buffers = new ArrayDeque<>();

	/** The number of non-event buffers to be announced to the downstream. */
	@GuardedBy("buffers")
	private int unannouncedBacklog;

	/** The read view to consume this subpartition. */
	private PipelinedSubpartitionView readView;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	/** The total number of buffers (both data and event buffers). */
	private long totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers). */
	private long totalNumberOfBytes;

	/** The collection of buffers which are spanned over by checkpoint barrier and needs to be persisted for snapshot. */
	private final List<Buffer> inflightBufferSnapshot = new ArrayList<>();

	/** Whether this subpartition is blocked by exactly once checkpoint and is waiting for resumption. */
	@GuardedBy("buffers")
	private boolean isBlockedByCheckpoint = false;

	// ------------------------------------------------------------------------

	PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException {
		boolean recycleBuffer = true;
		for (ReadResult readResult = ReadResult.HAS_MORE_DATA; readResult == ReadResult.HAS_MORE_DATA;) {
			BufferBuilder bufferBuilder = parent.getBufferPool().requestBufferBuilderBlocking(subpartitionInfo.getSubPartitionIdx());
			BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
			try {
				readResult = stateReader.readOutputData(subpartitionInfo, bufferBuilder);

				// check whether there are some states data filled in this time
				if (bufferConsumer.isDataAvailable()) {
					add(bufferConsumer, false, false);
					recycleBuffer = false;
					bufferBuilder.finish();
				}
			} finally {
				if (recycleBuffer) {
					bufferConsumer.close();
				}
			}
		}
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer, boolean isPriorityEvent) throws IOException {
		if (isPriorityEvent) {
			// TODO: use readView.notifyPriorityEvent for local channels
			return add(bufferConsumer, false, true);
		}
		return add(bufferConsumer, false, false);
	}

	@Override
	public void finish() throws IOException {
		add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true, false);
		LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
	}

	private boolean add(BufferConsumer bufferConsumer, boolean finish, boolean insertAsHead) {
		checkNotNull(bufferConsumer);

		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (isFinished || isReleased) {
				bufferConsumer.close();
				return false;
			}

			BufferConsumer previousBuffer = tryRemoveFinishedEmptyBuffer(insertAsHead);

			// Add the bufferConsumer and update the stats
			handleAddingBarrier(bufferConsumer, insertAsHead);
			updateStatistics(bufferConsumer);
			notifyDataAvailable = insertAsHead || finish || shouldNotifyDataAvailable(previousBuffer);

			tryIncreaseUnannouncedBacklog(previousBuffer);

			isFinished |= finish;
		}

		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return true;
	}

	private void handleAddingBarrier(BufferConsumer bufferConsumer, boolean insertAsHead) {
		assert Thread.holdsLock(buffers);
		if (insertAsHead) {
			checkState(inflightBufferSnapshot.isEmpty(), "Supporting only one concurrent checkpoint in unaligned " +
				"checkpoints");

			// Meanwhile prepare the collection of in-flight buffers which would be fetched in the next step later.
			for (BufferConsumer buffer : buffers) {
				try (BufferConsumer bc = buffer.copy()) {
					if (bc.isBuffer()) {
						inflightBufferSnapshot.add(bc.build());
					}
				}
			}

			buffers.addFirst(bufferConsumer);
		} else {
			buffers.add(bufferConsumer);
		}
	}

	@Override
	public List<Buffer> requestInflightBufferSnapshot() {
		List<Buffer> snapshot = new ArrayList<>(inflightBufferSnapshot);
		inflightBufferSnapshot.clear();
		return snapshot;
	}

	@Override
	public void release() {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			for (BufferConsumer buffer : buffers) {
				buffer.close();
			}
			buffers.clear();

			view = readView;
			readView = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;
		}

		LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

		if (view != null) {
			view.releaseAllResources();
		}
	}

	@Nullable
	BufferAndBacklog pollBuffer() {
		synchronized (buffers) {
			if (isBlockedByCheckpoint) {
				return null;
			}

			Buffer buffer = null;
			while (!buffers.isEmpty()) {
				BufferConsumer bufferConsumer = buffers.peek();

				buffer = bufferConsumer.build();

				checkState(bufferConsumer.isFinished() || buffers.size() == 1,
					"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

				if (buffers.size() == 1) {
					// turn off flushRequested flag if we drained all of the available data
					bufferConsumer.setFlushRequested(false);
				}

				if (bufferConsumer.isFinished()) {
					buffers.pop().close();
				}

				if (buffer.readableBytes() > 0) {
					break;
				}
				buffer.recycleBuffer();
				buffer = null;
				if (!bufferConsumer.isFinished()) {
					break;
				}
			}

			if (buffer == null) {
				return null;
			}

			if (buffer.getDataType().isBlockingUpstream()) {
				isBlockedByCheckpoint = true;
			}

			updateStatistics(buffer);
			// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
			// It will be reported for reading either on flush or when the number of buffers in the queue
			// will be 2 or more.
			return new BufferAndBacklog(
				buffer,
				isDataAvailableUnsafe(),
				getAndResetUnannouncedBacklogUnsafe(),
				isEventAvailableUnsafe());
		}
	}

	void resumeConsumption() {
		synchronized (buffers) {
			checkState(isBlockedByCheckpoint, "Should be blocked by checkpoint.");

			isBlockedByCheckpoint = false;
		}
	}

	@Override
	public int releaseMemory() {
		// The pipelined subpartition does not react to memory release requests.
		// The buffers will be recycled by the consuming task.
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			checkState(!isReleased);
			checkState(readView == null,
				"Subpartition %s of is being (or already has been) consumed, " +
				"but pipelined subpartitions can only be consumed once.",
				getSubPartitionIndex(),
				parent.getPartitionId());

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), getSubPartitionIndex(), parent.getPartitionId());

			readView = new PipelinedSubpartitionView(this, availabilityListener);
			notifyDataAvailable = isDataAvailableUnsafe();
		}
		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return readView;
	}

	public boolean isAvailable(int numCreditsAvailable) {
		synchronized (buffers) {
			if (numCreditsAvailable > 0) {
				return isDataAvailableUnsafe();
			}

			return isEventAvailableUnsafe();
		}
	}

	private boolean isDataAvailableUnsafe() {
		assert Thread.holdsLock(buffers);

		boolean flushRequested = !buffers.isEmpty() && buffers.peekFirst().isFlushRequested();
		return !isBlockedByCheckpoint && (flushRequested || getNumberOfFinishedBuffers() > 0);
	}

	private boolean isEventAvailableUnsafe() {
		assert Thread.holdsLock(buffers);

		return !isBlockedByCheckpoint && !buffers.isEmpty() && !buffers.peekFirst().isBuffer();
	}

	// ------------------------------------------------------------------------

	int getCurrentNumberOfBuffers() {
		return buffers.size();
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedSubpartition#%d [number of buffers: %d (%d bytes), unannounced backlog: %d, finished? %s, read view? %s]",
			getSubPartitionIndex(), numBuffers, numBytes, unannouncedBacklog, finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	@Override
	public void flush() {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (buffers.isEmpty()) {
				return;
			}
			// if there is more then 1 buffer, we already notified the reader
			// (at the latest when adding the second buffer), so we just need
			// to request the flushing of the last buffer in the queue if it
			// has available data
			BufferConsumer buffer = buffers.peekLast();
			boolean flushRequested = buffer.isBuffer() && !buffer.isFlushRequested() && buffer.isDataAvailable();
			notifyDataAvailable = !isBlockedByCheckpoint && buffers.size() == 1 && flushRequested;

			// increase the number of backlog by one and set the flush-requested
			// flag of the corresponding buffer to true if we request a flushing
			// of the last buffer in the queue
			if (flushRequested) {
				buffer.setFlushRequested(true);
				++unannouncedBacklog;
			}
		}
		if (notifyDataAvailable) {
			notifyDataAvailable();
		}
	}

	@Override
	protected long getTotalNumberOfBuffers() {
		return totalNumberOfBuffers;
	}

	@Override
	protected long getTotalNumberOfBytes() {
		return totalNumberOfBytes;
	}

	Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	private void updateStatistics(BufferConsumer buffer) {
		totalNumberOfBuffers++;
	}

	private void updateStatistics(Buffer buffer) {
		totalNumberOfBytes += buffer.getSize();
	}

	/**
	 * Removes and recycles the last buffer in the queue if it is an finished buffer without any consumable
	 * data, after which, the reader does not need any available credit to handle this finished empty buffer.
	 * For example, if the new buffer is an event and the previous buffer is finished but without any data,
	 * after we remove the empty one, the reader is able to process the event without any available credit.
	 *
	 * @param insertAsHead Whether the new added buffer is a priority-event.
	 * @return The last buffer in the queue before the new buffer is added if it is not removed.
	 */
	@GuardedBy("buffers")
	private BufferConsumer tryRemoveFinishedEmptyBuffer(boolean insertAsHead) {
		assert Thread.holdsLock(buffers);

		BufferConsumer prevBuffer = insertAsHead ? null : buffers.peekLast();
		if (prevBuffer != null && prevBuffer.getCurrentReaderPosition() > 0 && !prevBuffer.isDataAvailable()) {
			checkState(prevBuffer.isBuffer(), "The last buffer must not be an event.");
			buffers.pollLast().close();
			prevBuffer = null;
		}
		return prevBuffer;
	}

	/**
	 * Tries to increase the number of unannounced backlog for the previous buffer after adding a new buffer.
	 *
	 * @param previousBuffer The last buffer in the queue before the new buffer is added.
	 */
	@GuardedBy("buffers")
	private void tryIncreaseUnannouncedBacklog(BufferConsumer previousBuffer) {
		assert Thread.holdsLock(buffers);

		if (previousBuffer != null && !previousBuffer.isFlushRequested()) {
			++unannouncedBacklog;
		}
	}

	@Override
	public int getUnannouncedBacklog() {
		return unannouncedBacklog;
	}

	int getAndResetUnannouncedBacklog() {
		synchronized (buffers) {
			return getAndResetUnannouncedBacklogUnsafe();
		}
	}

	private int getAndResetUnannouncedBacklogUnsafe() {
		if (isBlockedByCheckpoint) {
			return 0;
		}
		int numBacklog = unannouncedBacklog;
		unannouncedBacklog = 0;
		return numBacklog;
	}

	private boolean shouldNotifyDataAvailable(BufferConsumer previousBuffer) {
		boolean noFlushRequested = previousBuffer == null || !previousBuffer.isFlushRequested();
		// Notify only when we added first finished buffer.
		return readView != null && noFlushRequested && !isBlockedByCheckpoint && getNumberOfFinishedBuffers() == 1;
	}

	private void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}

	private int getNumberOfFinishedBuffers() {
		assert Thread.holdsLock(buffers);

		// NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
		// worst-case: a single finished buffer sits around until the next flush() call
		// (but we do not offer stronger guarantees anyway)
		if (buffers.size() == 1 && buffers.peekLast().isFinished()) {
			return 1;
		}

		// We assume that only last buffer is not finished.
		return Math.max(0, buffers.size() - 1);
	}
}
