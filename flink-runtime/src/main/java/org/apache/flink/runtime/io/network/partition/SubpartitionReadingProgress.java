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

/** Progress of reading subpartition data. */
public class SubpartitionReadingProgress {

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Target subpartition to read. */
    private final int targetSubpartition;

    /** Next data region to read. */
    private int nextRegionIndex;

    /** Current file offset to read. */
    private long currentStartOffset;

    /** Remaining bytes to read of current region. */
    private long currentRegionRemainingBytes;

    public SubpartitionReadingProgress(PartitionedFile partitionedFile, int targetSubpartition) {
        checkArgument(targetSubpartition >= 0, "Negative subpartition index.");
        this.targetSubpartition = targetSubpartition;
        this.partitionedFile = checkNotNull(partitionedFile);
    }

    public int getNextRegionIndex() {
        return nextRegionIndex;
    }

    public long getCurrentStartOffset() {
        return currentStartOffset;
    }

    public long getCurrentRegionRemainingBytes() {
        return currentRegionRemainingBytes;
    }

    public long getCurrentEndOffset() {
        return currentStartOffset + currentRegionRemainingBytes;
    }

    boolean updateReadingProgress(
            long numBytesRead, ByteBuffer indexEntryBuf, FileChannel indexFileChannel)
            throws IOException {
        currentRegionRemainingBytes -= numBytesRead;
        currentStartOffset += numBytesRead;

        while (currentRegionRemainingBytes <= 0
                && nextRegionIndex < partitionedFile.getNumRegions()) {
            partitionedFile.getIndexEntry(
                    indexFileChannel, indexEntryBuf, nextRegionIndex, targetSubpartition);
            currentStartOffset = indexEntryBuf.getLong();
            currentRegionRemainingBytes = indexEntryBuf.getLong();
            ++nextRegionIndex;
        }
        return currentRegionRemainingBytes > 0;
    }

    boolean currentRegionHasRemaining() {
        return currentRegionRemainingBytes > 0;
    }
}
