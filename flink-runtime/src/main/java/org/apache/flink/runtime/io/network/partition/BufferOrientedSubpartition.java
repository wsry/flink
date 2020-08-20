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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import java.io.IOException;

public abstract class BufferOrientedSubpartition extends ResultSubpartition {

	public BufferOrientedSubpartition(int index, BufferOrientedResultPartition parent) {
		super(index, parent);
	}

	/**
	 * Adds the given buffer.
	 *
	 * <p>The request may be executed synchronously, or asynchronously, depending on the
	 * implementation.
	 *
	 * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must be in finished
	 * state. Because of the performance reasons, this is only enforced during the data reading.
	 *
	 * @param bufferConsumer
	 * 		the buffer to add (transferring ownership to this writer)
	 * @param isPriorityEvent
	 * @return true if operation succeeded and bufferConsumer was enqueued for consumption.
	 * @throws IOException
	 * 		thrown in case of errors while adding the buffer
	 */
	public abstract boolean add(BufferConsumer bufferConsumer, boolean isPriorityEvent) throws IOException;

	/**
	 * Adds the given buffer.
	 *
	 * <p>The request may be executed synchronously, or asynchronously, depending on the
	 * implementation.
	 *
	 * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must be in finished
	 * state. Because of the performance reasons, this is only enforced during the data reading.
	 *
	 * @param bufferConsumer
	 * 		the buffer to add (transferring ownership to this writer)
	 * @return true if operation succeeded and bufferConsumer was enqueued for consumption.
	 * @throws IOException
	 * 		thrown in case of errors while adding the buffer
	 */
	public boolean add(BufferConsumer bufferConsumer) throws IOException {
		return add(bufferConsumer, false);
	}

	/**
	 * Whether the buffer can be compressed or not. Note that event is not compressed because it
	 * is usually small and the size can become even larger after compression.
	 */
	protected boolean canBeCompressed(Buffer buffer) {
		return ((BufferOrientedResultPartition) parent).bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
	}

	/**
	 * Gets the total numbers of buffers (data buffers plus events).
	 */
	protected abstract long getTotalNumberOfBuffers();
}
