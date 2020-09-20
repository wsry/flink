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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for writing and reading {@link PartitionedFile} with {@link PartitionedFileWriter}
 * and {@link PartitionedFileReader}.
 */
public class PartitionedFileWriteReadTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testWriteAndReadPartitionedFile() throws Exception {
		int numSubpartitions = 10;
		int bufferSize = 1024;
		int numBuffers = 1000;
		Random random = new Random();

		List<Buffer>[] buffersWritten = new ArrayList[numSubpartitions];
		List<Buffer>[] buffersRead = new ArrayList[numSubpartitions];
		for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
			buffersWritten[subpartition] = new ArrayList<>();
			buffersRead[subpartition] = new ArrayList<>();
		}

		for (int i = 0; i < numBuffers; ++i) {
			boolean isBuffer = random.nextBoolean();
			Buffer.DataType dataType = isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;

			int dataSize = random.nextInt(bufferSize) + 1;
			byte[] data = new byte[dataSize];
			Buffer buffer = new NetworkBuffer(MemorySegmentFactory.wrap(data), (buf) -> {}, dataType, dataSize);

			int subpartition = random.nextInt(numSubpartitions);
			buffersWritten[subpartition].add(buffer);
		}

		PartitionedFileWriter fileWriter = createAndOpenPartitionedFileWriter(numSubpartitions);
		for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
			for (Buffer buffer: buffersWritten[subpartition]) {
				fileWriter.writeBuffer(buffer, subpartition);
			}
		}
		PartitionedFile partitionedFile = fileWriter.finish();

		for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
			PartitionedFileReader fileReader = new PartitionedFileReader(partitionedFile, subpartition);
			fileReader.open();
			while (fileReader.hasRemaining()) {
				MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
				Buffer buffer = fileReader.readBuffer(readBuffer, (buf) -> {});
				buffersRead[subpartition].add(buffer);
			}
			fileReader.close();
		}

		for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
			assertEquals(buffersWritten[subpartition].size(), buffersRead[subpartition].size());
			for (int i = 0; i < buffersWritten[subpartition].size(); ++i) {
				Buffer bufferWritten = buffersWritten[subpartition].get(i);
				Buffer bufferRead = buffersRead[subpartition].get(i);
				assertEquals(bufferWritten.getDataType(), bufferRead.getDataType());
				assertEquals(bufferWritten.getNioBufferReadable(), bufferRead.getNioBufferReadable());
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWriteNotInSubpartitionOrder() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createAndOpenPartitionedFileWriter(2);
		try {
			MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

			NetworkBuffer buffer1 = new NetworkBuffer(segment, (buf) -> {});
			partitionedFileWriter.writeBuffer(buffer1, 1);

			NetworkBuffer buffer2 = new NetworkBuffer(segment, (buf) -> {});
			partitionedFileWriter.writeBuffer(buffer2, 0);
		} finally {
			partitionedFileWriter.finish();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testWriteFinishedPartitionedFile() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();

		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
		NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

		partitionedFileWriter.writeBuffer(buffer, 0);
	}

	@Test(expected = IllegalStateException.class)
	public void testWritePartitionedFileBeforeOpen() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(1);

		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
		NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

		partitionedFileWriter.writeBuffer(buffer, 0);
	}

	@Test(expected = IllegalStateException.class)
	public void testOpenPartitionedFileWriterTwice() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createAndOpenPartitionedFileWriter(1);
		try {
			partitionedFileWriter.open();
		} finally {
			partitionedFileWriter.finish();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testFinishPartitionedFileWriterTwice() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();
		partitionedFileWriter.finish();
	}

	@Test(expected = IllegalStateException.class)
	public void testFinishPartitionedFileWriterBeforeOpen() throws Exception {
		PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(1);
		partitionedFileWriter.finish();
	}

	@Test(expected = IllegalStateException.class)
	public void testOpenPartitionedFileReaderTwice() throws Exception {
		try (PartitionedFileReader partitionedFileReader = createAndOpenPartitionedFiledReader()) {
			partitionedFileReader.open();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testOpenClosedPartitionedFileReader() throws Exception {
		PartitionedFileReader partitionedFileReader = createAndOpenPartitionedFiledReader();
		partitionedFileReader.close();
		partitionedFileReader.open();
	}

	@Test(expected = IllegalStateException.class)
	public void testReadPartitionedFileBeforeOpen() throws Exception {
		PartitionedFileReader partitionedFileReader = createPartitionedFiledReader();

		MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);
		partitionedFileReader.readBuffer(target, FreeingBufferRecycler.INSTANCE);
	}

	@Test(expected = IllegalStateException.class)
	public void testReadClosedPartitionedFile() throws Exception {
		PartitionedFileReader partitionedFileReader = createAndClosePartitionedFiledReader();

		MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);
		partitionedFileReader.readBuffer(target, FreeingBufferRecycler.INSTANCE);
	}

	@Test
	public void testReadEmptyPartitionedFile() throws Exception {
		try (PartitionedFileReader partitionedFileReader = createAndOpenPartitionedFiledReader()) {
			MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);
			assertNull(partitionedFileReader.readBuffer(target, FreeingBufferRecycler.INSTANCE));
		}
	}

	private PartitionedFileReader createAndClosePartitionedFiledReader() throws IOException {
		PartitionedFileReader fileReader = createAndOpenPartitionedFiledReader();
		fileReader.close();
		return fileReader;
	}

	private PartitionedFileReader createAndOpenPartitionedFiledReader() throws IOException {
		PartitionedFileReader fileReader = createPartitionedFiledReader();
		fileReader.open();
		return fileReader;
	}

	private PartitionedFileReader createPartitionedFiledReader() throws IOException {
		PartitionedFile partitionedFile = createPartitionedFile();
		return new PartitionedFileReader(partitionedFile, 1);
	}

	private PartitionedFile createPartitionedFile() throws IOException {
		PartitionedFileWriter partitionedFileWriter = createAndOpenPartitionedFileWriter(2);
		return partitionedFileWriter.finish();
	}

	private PartitionedFileWriter createPartitionedFileWriter(int numSubpartitions) throws IOException {
		String basePath = temporaryFolder.newFile().getPath();
		return new PartitionedFileWriter(basePath, numSubpartitions);
	}

	private PartitionedFileWriter createAndOpenPartitionedFileWriter(int numSubpartitions) throws IOException {
		PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(numSubpartitions);
		partitionedFileWriter.open();
		return partitionedFileWriter;
	}

	private PartitionedFileWriter createAndFinishPartitionedFileWriter() throws IOException {
		PartitionedFileWriter partitionedFileWriter = createAndOpenPartitionedFileWriter(1);
		partitionedFileWriter.finish();
		return partitionedFileWriter;
	}
}
