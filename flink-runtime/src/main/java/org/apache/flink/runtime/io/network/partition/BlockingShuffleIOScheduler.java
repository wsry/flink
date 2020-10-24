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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 *
 */
public class BlockingShuffleIOScheduler extends Thread implements BufferRecycler {

	private static final Logger LOG = LoggerFactory.getLogger(BlockingShuffleIOScheduler.class);

	private static final int NUM_READ_QUOTAS = 256;

	private final Object lock;

	private final int bufferSize;

	private final LinkedBlockingQueue<MemorySegment> readBuffers = new LinkedBlockingQueue<>();

	@GuardedBy("lock")
	private final Queue<SortMergeSubpartitionReader> subpartitionReaders = new PriorityQueue<>();

	@GuardedBy("lock")
	private FileChannel dataFileChannel;

	@GuardedBy("lock")
	private FileChannel indexFileChannel;

	@GuardedBy("lock")
	private boolean isReleased;

	@GuardedBy("lock")
	private Throwable failureCause;

	public BlockingShuffleIOScheduler(int bufferSize, Object lock) {
		this.bufferSize = bufferSize;
		this.lock = checkNotNull(lock);
	}

	@Override
	public void run() {
		List<SortMergeSubpartitionReader> availableReaders;
		while (true) {
			synchronized (lock) {
				if (!isReleased && readBuffers.isEmpty()) {
					ExceptionUtils.suppressExceptions(lock::wait);
				}

				availableReaders = new ArrayList<>(subpartitionReaders);
				if (isReleased) {
					break;
				}
			}

			Collections.sort(availableReaders);
			for (SortMergeSubpartitionReader subpartitionReader: availableReaders) {
				Throwable exception = null;
				try {
					if (!readBuffersFromCurrentRegion(subpartitionReader)) {
						break;
					}
				} catch (Throwable throwable) {
					LOG.error("Failed to read shuffle data.", throwable);
					exception = throwable;
				}

				if (exception != null) {
					try {
						subpartitionReader.fail(exception);
					} catch (Throwable throwable) {
						LOG.error("Failed to release subpartition reader.", throwable);
					}
				}
			}
		}

		for (SortMergeSubpartitionReader subpartitionReader : availableReaders) {
			try {
				subpartitionReader.fail(failureCause);
			} catch (Throwable throwable) {
				LOG.error("Failed to release subpartition reader.", throwable);
			}
		}
	}

	private boolean readBuffersFromCurrentRegion(SortMergeSubpartitionReader subpartitionReader) throws IOException {
		PartitionedFileReader fileReader = subpartitionReader.getFileReader();

		while (fileReader.getCurrentRegionRemainingBuffers() > 0) {
			MemorySegment segment;
			if ((segment = readBuffers.poll()) == null) {
				break;
			}

			Buffer buffer;
			try {
				buffer = checkNotNull(fileReader.readBuffer(segment, this), "Failed to read data.");
			} catch (Throwable throwable) {
				recycle(segment);
				throw throwable;
			}
			subpartitionReader.addBuffer(buffer);
		}

		boolean currentRegionFinished = fileReader.getCurrentRegionRemainingBuffers() == 0;
		fileReader.moveToNextReadableRegion();
		return currentRegionFinished;
	}

	public SortMergeSubpartitionReader crateSubpartitionReader(
			SortMergeResultPartition resultPartition,
			PartitionedFile resultFile,
			BufferAvailabilityListener availabilityListener,
			int targetSubpartition) throws IOException {
		synchronized (lock) {
			checkState(!isReleased, "Partition is already released.");

			if (subpartitionReaders.isEmpty()) {
				while (readBuffers.size() < NUM_READ_QUOTAS) {
					readBuffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize, null));
				}
				openFileChannels(resultFile);
			}

			PartitionedFileReader fileReader = new PartitionedFileReader(
				resultFile, targetSubpartition, dataFileChannel, indexFileChannel);

			SortMergeSubpartitionReader subpartitionReader = new SortMergeSubpartitionReader(
				resultPartition,
				availabilityListener,
				fileReader);
			subpartitionReaders.add(subpartitionReader);

			if (subpartitionReaders.size() == 1) {
				lock.notify();
			}
			return subpartitionReader;
		}
	}

	public void releaseSubpartitionReader(SortMergeSubpartitionReader subpartitionReader) {
		synchronized (lock) {
			if (!subpartitionReaders.remove(subpartitionReader)) {
				return;
			}

			if (subpartitionReaders.isEmpty()) {
				readBuffers.clear();
				closeFileChannelsQuietly();
			}
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		synchronized (lock) {
			if (subpartitionReaders.isEmpty() || isReleased || readBuffers.size() >= NUM_READ_QUOTAS) {
				return;
			}

			readBuffers.add(segment);
			if (readBuffers.size() == 1) {
				lock.notify();
			}
		}
	}

	public void release(Throwable throwable) {
		synchronized (lock) {
			if (!isReleased) {
				isReleased = true;
				failureCause = throwable;

				lock.notify();
			}
		}
	}

	private void closeFileChannelsQuietly() {
		IOUtils.closeQuietly(dataFileChannel);
		IOUtils.closeQuietly(indexFileChannel);
	}

	private void openFileChannels(PartitionedFile partitionedFile) throws IOException {
		try {
			dataFileChannel = FileChannel.open(partitionedFile.getDataFilePath(), StandardOpenOption.READ);
			indexFileChannel = FileChannel.open(partitionedFile.getIndexFilePath(), StandardOpenOption.READ);
		} catch (Throwable throwable) {
			closeFileChannelsQuietly();
			throw throwable;
		}
	}
}
