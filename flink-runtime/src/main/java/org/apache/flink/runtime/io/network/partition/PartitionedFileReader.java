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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readByteBufferFully;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Reader which can read all data of the target subpartition from a {@link PartitionedFile}. */
class PartitionedFileReader {

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Target subpartition to read. */
    private final int targetSubpartition;

    /** Data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    /** Next data region to be read. */
    private int nextRegionToRead;

    /** Next file offset to be read. */
    private long nextOffsetToRead;

    /** Remaining data size in bytes in the current data region to read. */
    private long currentRegionRemainingBytes;

    private PartialBuffer partialBuffer;

    PartitionedFileReader(
            PartitionedFile partitionedFile,
            int targetSubpartition,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel) {
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.targetSubpartition = targetSubpartition;
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;

        this.indexEntryBuf = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBuf);
    }

    private void moveToNextReadableRegion() throws IOException {
        while (currentRegionRemainingBytes <= 0
                && nextRegionToRead < partitionedFile.getNumRegions()) {
            partitionedFile.getIndexEntry(
                    indexFileChannel, indexEntryBuf, nextRegionToRead, targetSubpartition);
            nextOffsetToRead = indexEntryBuf.getLong();
            currentRegionRemainingBytes = indexEntryBuf.getLong();
            ++nextRegionToRead;
        }
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * @param target The target {@link MemorySegment} to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @return A {@link Buffer} containing the data read.
     */
    @Nullable
    List<Buffer> readCurrentRegion(MemorySegment target, BufferRecycler recycler)
            throws IOException {
        if (currentRegionRemainingBytes == 0) {
            return null;
        }

        int segmentOffset = 0;
        if (partialBuffer != null) {
            target.put(
                    0,
                    partialBuffer.segment.wrap(partialBuffer.offset, partialBuffer.length),
                    partialBuffer.length);
            segmentOffset = partialBuffer.length;
            partialBuffer.recycle();
            partialBuffer = null;
        }

        dataFileChannel.position(nextOffsetToRead);
        int bytesRead =
                MathUtils.checkedDownCast(
                        Math.min(currentRegionRemainingBytes, target.size() - segmentOffset));
        readByteBufferFully(dataFileChannel, target.wrap(segmentOffset, bytesRead));
        nextOffsetToRead += bytesRead;
        currentRegionRemainingBytes -= bytesRead;

        int position = 0;
        int totalBytes = segmentOffset + bytesRead;
        List<Buffer> buffers = new ArrayList<>(16);
        AtomicInteger referenceCount = new AtomicInteger(0);
        BufferRecycler recyclerWrapper =
                segment -> {
                    checkArgument(segment == target, "Invalid buffer recycle.");
                    int count = referenceCount.decrementAndGet();
                    if (count == 0) {
                        recycler.recycle(segment);
                    } else if (count < 0) {
                        throw new IllegalStateException("Illegal reference count.");
                    }
                };
        while (position < totalBytes) {
            int remainingBytes = totalBytes - position;
            if (remainingBytes < BufferReaderWriterUtil.HEADER_LENGTH) {
                referenceCount.incrementAndGet();
                partialBuffer =
                        new PartialBuffer(position, remainingBytes, target, recyclerWrapper);
                break;
            }

            ByteBuffer headerBuffer = target.wrap(position, BufferReaderWriterUtil.HEADER_LENGTH);
            BufferReaderWriterUtil.BufferHeader header = parseBufferHeader(headerBuffer);
            if (remainingBytes - BufferReaderWriterUtil.HEADER_LENGTH < header.getLength()) {
                referenceCount.incrementAndGet();
                partialBuffer =
                        new PartialBuffer(position, remainingBytes, target, recyclerWrapper);
                break;
            } else {
                int readerIndex = position + BufferReaderWriterUtil.HEADER_LENGTH;
                position = readerIndex + header.getLength();
                referenceCount.incrementAndGet();
                NetworkBuffer buffer =
                        new NetworkBuffer(
                                target, recyclerWrapper, header.getDataType(), false, position);
                Buffer slicedBuffer = buffer.readOnlySlice(readerIndex, header.getLength());
                slicedBuffer.setCompressed(header.isCompressed());
                buffers.add(slicedBuffer);
            }
        }
        return buffers;
    }

    boolean hasRemaining() throws IOException {
        moveToNextReadableRegion();
        return currentRegionRemainingBytes > 0;
    }

    /** Gets read priority of this file reader. Smaller value indicates higher priority. */
    long getPriority() {
        return nextOffsetToRead;
    }

    private static class PartialBuffer {

        private final int offset;

        private final int length;

        private final MemorySegment segment;

        private final BufferRecycler recycler;

        PartialBuffer(int offset, int length, MemorySegment segment, BufferRecycler recycler) {
            this.offset = offset;
            this.length = length;
            this.segment = checkNotNull(segment);
            this.recycler = checkNotNull(recycler);
        }

        void recycle() {
            recycler.recycle(segment);
        }
    }
}
