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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link #add(BufferConsumer)} adds a finished {@link BufferConsumer} or a second
 * {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via
 * {@link #createReadView(BufferAvailabilityListener)} of new data availability. Except by calling
 * {@link #flush()} explicitly, we always only notify when the first finished buffer turns up and
 * then, the reader has to drain the buffers via {@link #pollBuffer()} until its return value shows
 * no more buffers being available. This results in a buffer queue which is either empty or has an
 * unfinished {@link BufferConsumer} left from which the notifications will eventually start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notification} for any
 * {@link BufferConsumer} present in the queue.
 */
class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<BufferConsumer> buffers = new ArrayDeque<>();

	private int unannouncedBacklog = 0;

	/** The read view to consume this subpartition. */
	private PipelinedSubpartitionView readView;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	@GuardedBy("buffers")
	private boolean flushRequested;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	/** The total number of buffers (both data and event buffers). */
	private long totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers). */
	private long totalNumberOfBytes;

	private boolean alreadyNotifiedAvailable = false;

	// ------------------------------------------------------------------------

	PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer) {
		return add(bufferConsumer, false);
	}

	@Override
	public void finish() throws IOException {
		add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true);
		LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
	}

	private boolean add(BufferConsumer bufferConsumer, boolean finish) {
		checkNotNull(bufferConsumer);

		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (isFinished || isReleased) {
				bufferConsumer.close();
				return false;
			}

			// Add the bufferConsumer and update the stats
			buffers.add(bufferConsumer);
			updateStatistics(bufferConsumer);
			if (bufferConsumer.isBuffer()) {
				++unannouncedBacklog;
			}

			notifyDataAvailable = shouldNotifyDataAvailable();
			alreadyNotifiedAvailable = alreadyNotifiedAvailable || notifyDataAvailable;
			if (notifyDataAvailable && isPartialBufferAndDataAvailable(buffers.peekFirst())) {
				++unannouncedBacklog;
			}

			isFinished |= finish;
		}

		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return true;
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
			checkState(!buffers.isEmpty());

			BufferConsumer bufferConsumer = buffers.peek();
			boolean shouldBlocking = bufferConsumer.isBlockingEvent();
			Buffer buffer = bufferConsumer.build();

			checkState(bufferConsumer.isFinished() || (buffers.size() == 1 && buffer.readableBytes() > 0),
				"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

			if (bufferConsumer.isFinished()) {
				buffers.pop().close();
			}

			boolean nextBufferIsEvent = nextBufferIsEventUnsafe();
			alreadyNotifiedAvailable = buffers.size() > 1 || nextBufferIsEvent;

			updateStatistics(buffer);
			// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
			// It will be reported for reading either on flush or when the number of buffers in the queue
			// will be 2 or more.
			return new BufferAndBacklog(
				buffer,
				alreadyNotifiedAvailable,
				getAndResetUnannouncedBacklogUnsafe(),
				nextBufferIsEvent,
				shouldBlocking);
		}
	}

	boolean nextBufferIsFinishedEmptyOrEvent() {
		synchronized (buffers) {
			return nextBufferIsEventUnsafe() || nextBufferIsFinishedEmptyUnsafe();
		}
	}

	private boolean nextBufferIsEventUnsafe() {
		assert Thread.holdsLock(buffers);

		return !buffers.isEmpty() && !buffers.peekFirst().isBuffer();
	}

	private boolean nextBufferIsFinishedEmptyUnsafe() {
		assert Thread.holdsLock(buffers);

		return buffers.size() > 1 && !buffers.peekFirst().isDataAvailable();
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
					"but pipelined subpartitions can only be consumed once.", index, parent.getPartitionId());

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), index, parent.getPartitionId());

			readView = new PipelinedSubpartitionView(this, availabilityListener);
			notifyDataAvailable = shouldNotifyDataAvailable();
			alreadyNotifiedAvailable = notifyDataAvailable;
		}
		if (notifyDataAvailable) {
			notifyDataAvailable();
		}

		return readView;
	}

	public boolean isAvailable() {
		synchronized (buffers) {
			return alreadyNotifiedAvailable;
		}
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
			"PipelinedSubpartition#%d [number of buffers: %d (%d bytes), unannounced backlog: %d, finished? %s," +
				" read view? %s]", index, numBuffers, numBytes, unannouncedBacklog, finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	@Override
	public int getAndResetUnannouncedBacklog() {
		synchronized (buffers) {
			return getAndResetUnannouncedBacklogUnsafe();
		}
	}

	private int getAndResetUnannouncedBacklogUnsafe() {
		assert Thread.holdsLock(buffers);

		int backlog = unannouncedBacklog;
		unannouncedBacklog = 0;
		return backlog;
	}

	@Override
	public void flush() {
		final boolean notifyDataAvailable;
		synchronized (buffers) {
			flushRequested = true;

			if (buffers.isEmpty() || readView == null || alreadyNotifiedAvailable) {
				return;
			}
			// if there is more then 1 buffer, we already notified the reader
			// (at the latest when adding the second buffer)
			checkState(buffers.size() == 1);
			BufferConsumer buffer = buffers.peekFirst();
			notifyDataAvailable = buffer.isDataAvailable();
			alreadyNotifiedAvailable = notifyDataAvailable;
			if (notifyDataAvailable && isPartialBuffer(buffer)) {
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

	private boolean shouldNotifyDataAvailable() {
		// Notify only when we added first finished buffer.
		return readView != null && !alreadyNotifiedAvailable && (buffers.size() > 1 || nextBufferIsEventUnsafe());
	}

	private void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}

	private boolean isPartialBuffer(BufferConsumer bufferConsumer) {
		return bufferConsumer.isBuffer() && bufferConsumer.getCurrentReaderPosition() > 0;
	}

	private boolean isPartialBufferAndDataAvailable(BufferConsumer bufferConsumer) {
		return isPartialBuffer(bufferConsumer) && bufferConsumer.isDataAvailable();
	}
}
