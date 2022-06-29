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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Progress of reading subpartition data. */
public class SubpartitionReadingProgress {

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    private final int targetSubpartition;

    private int nextRegionIndex;

    private long fileOffset;

    private long currentRegionRemainingBytes;

    public SubpartitionReadingProgress(
            PartitionedFile partitionedFile, FileChannel indexFileChannel, int targetSubpartition) {
        checkArgument(targetSubpartition >= 0, "Negative subpartition index.");
        this.targetSubpartition = targetSubpartition;

        this.partitionedFile = checkNotNull(partitionedFile);

        this.indexFileChannel = checkNotNull(indexFileChannel);
        checkState(indexFileChannel.isOpen(), "Index file channel is not opened.");
    }

    public int getNextRegionIndex() {
        return nextRegionIndex;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public long getCurrentRegionRemainingBytes() {
        return currentRegionRemainingBytes;
    }

    boolean updateReadingProgress(long numBytesRead, ByteBuffer indexEntryBuf) throws IOException {
        currentRegionRemainingBytes -= numBytesRead;
        fileOffset += numBytesRead;

        System.out.println(
                "updateReadingProgress: "
                        + currentRegionRemainingBytes
                        + " "
                        + numBytesRead
                        + " "
                        + nextRegionIndex);
        while (currentRegionRemainingBytes <= 0
                && nextRegionIndex < partitionedFile.getNumRegions()) {
            partitionedFile.getIndexEntry(
                    indexFileChannel, indexEntryBuf, nextRegionIndex, targetSubpartition);
            fileOffset = indexEntryBuf.getLong();
            currentRegionRemainingBytes = indexEntryBuf.getLong();
            ++nextRegionIndex;
        }
        System.out.println(
                "currentRegionRemainingBytes: "
                        + currentRegionRemainingBytes
                        + " "
                        + fileOffset
                        + " "
                        + nextRegionIndex);
        return currentRegionRemainingBytes > 0;
    }

    boolean isDataInRange(long startOffset, long length) {
        return fileOffset <= startOffset
                && fileOffset + currentRegionRemainingBytes >= startOffset + length;
    }
}
