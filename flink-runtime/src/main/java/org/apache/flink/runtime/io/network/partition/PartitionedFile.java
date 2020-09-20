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

import org.apache.flink.util.ExceptionUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PartitionedFile} stores data of all subpartitions in subpartition index order.
 */
public class PartitionedFile {

	/** Number of subpartitions of this partitioned file. */
	private final int numSubpartitions;

	/** Data file stores data of all subpartitions in channel index order. */
	private final Path dataFile;

	/** Subpartition index information of this partitioned file. */
	private final PartitionedFileIndex fileIndex;

	public PartitionedFile(int numSubpartitions, Path dataFile, PartitionedFileIndex fileIndex) {
		this.numSubpartitions = numSubpartitions;
		this.dataFile = checkNotNull(dataFile);
		this.fileIndex = checkNotNull(fileIndex);
	}

	public Path getDataFile() {
		return dataFile;
	}

	/**
	 * Returns the starting offset of the given subpartition in this {@link PartitionedFile}.
	 */
	public long getStartingOffset(int subpartitionIndex) {
		checkArgument(subpartitionIndex >= 0 && subpartitionIndex < numSubpartitions, "Illegal subpartition.");

		return fileIndex.startingOffsets[subpartitionIndex];
	}

	/**
	 * Returns the number of buffers of the given subpartition in this {@link PartitionedFile}.
	 */
	public int getNumBuffers(int subpartitionIndex) {
		checkArgument(subpartitionIndex >= 0 && subpartitionIndex < numSubpartitions, "Illegal subpartition.");

		return fileIndex.numBuffers[subpartitionIndex];
	}

	public void deleteQuietly() {
		ExceptionUtils.suppressExceptions(() -> Files.delete(dataFile));
	}

	@Override
	public String toString() {
		return "PartitionedFile{numSubpartitions=" + numSubpartitions + ", dataFile=" + dataFile + '}';
	}

	/**
	 * Subpartition index information for {@link PartitionedFile}.
	 */
	public static class PartitionedFileIndex {

		/** Number of buffers for each subpartition in the corresponding partitioned file.*/
		private final int[] numBuffers;

		/** Starting offset for each subpartition in the corresponding partitioned file. */
		private final long[] startingOffsets;

		public PartitionedFileIndex(int[] numBuffers, long[] startingOffsets) {
			this.numBuffers = checkNotNull(numBuffers);
			this.startingOffsets = checkNotNull(startingOffsets);
		}
	}
}
