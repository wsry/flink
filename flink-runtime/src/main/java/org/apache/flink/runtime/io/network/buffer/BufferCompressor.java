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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.compression.InsufficientBufferException;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Compressor for {@link Buffer}.
 */
public class BufferCompressor {
	private final BlockCompressor compressorBackend;

	private final byte[] heapBuffer;

	public BufferCompressor(int bufferSize, String factoryName) {
		checkArgument(bufferSize > 0);
		this.heapBuffer = new byte[2 * bufferSize];
		this.compressorBackend = BlockCompressionFactory.createBlockCompressionFactory(factoryName).getCompressor();
	}

	public Buffer compress(Buffer buffer, int offset, int length) {
		if (length <= 0) {
			return buffer;
		}
		try {
			MemorySegment segment = buffer.getMemorySegment();
			int compressedLen;
			if (segment.isOffHeap()) {
				final ByteBuffer src = buffer.getNioBuffer(offset, length);
				compressedLen = compressorBackend.compress(src, 0, length, ByteBuffer.wrap(heapBuffer), 0);
			} else {
				final byte[] src = segment.getArray();
				compressedLen = compressorBackend.compress(src, offset, length, heapBuffer, 0);
			}
			if (compressedLen < length) {
				HeapMemorySegment heapMemorySegment = HeapMemorySegment.FACTORY.wrap(heapBuffer);
				NetworkBuffer compressedBuffer = new NetworkBuffer(heapMemorySegment, FreeingBufferRecycler.INSTANCE);
				compressedBuffer.setCompressed(true);
				compressedBuffer.setSize(compressedLen);
				return compressedBuffer;
			}
			return buffer;
		} catch (InsufficientBufferException e) {
			return buffer;
		}
	}
}
