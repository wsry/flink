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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Outbound message to be sent from the server to the client.
 */
public abstract class ServerOutboundMessage {
	protected final InputChannelID receiverId;
	protected final int backlog;
	private final boolean moreAvailable;

	ServerOutboundMessage(InputChannelID receiverId, int backlog, boolean moreAvailable) {
		checkArgument(backlog >= 0, "Number of backlog must be non-negative.");
		this.receiverId = checkNotNull(receiverId);
		this.backlog = backlog;
		this.moreAvailable = moreAvailable;
	}

	abstract Object build();

	public boolean isMoreAvailable() {
		return moreAvailable;
	}

	void recycle() {
	}

	static class BufferResponseMessage extends ServerOutboundMessage {
		private final Buffer buffer;
		private final int sequenceNumber;

		BufferResponseMessage(
			Buffer buffer,
			InputChannelID receiverId,
			int sequenceNumber,
			int backlog,
			boolean moreAvailable) {
			super(receiverId, backlog, moreAvailable);
			this.buffer = checkNotNull(buffer);
			this.sequenceNumber = sequenceNumber;
		}

		@Override
		Object build() {
			return new NettyMessage.BufferResponse(buffer, sequenceNumber, receiverId, backlog);
		}

		@Override
		void recycle() {
			buffer.recycleBuffer();
		}
	}

	static class AddBacklogMessage extends ServerOutboundMessage {

		AddBacklogMessage(InputChannelID receiverId, int backlog) {
			super(receiverId, backlog, false);
		}

		@Override
		Object build() {
			return new NettyMessage.AddBacklog(backlog, receiverId);
		}
	}
}
