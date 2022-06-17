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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link FileEnumerator} implementation for hive source, which generates splits based on {@link
 * HiveTablePartition}s.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSourceFileEnumerator.class);

    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;
    private final JobConf jobConf;
    private final ReadableConfig flinkConf;

    public HiveSourceFileEnumerator(
            List<HiveTablePartition> partitions, JobConf jobConf, ReadableConfig flinkConf) {
        this.partitions = partitions;
        this.jobConf = jobConf;
        this.flinkConf = flinkConf;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(createInputSplits(minDesiredSplits, partitions, jobConf, flinkConf));
    }

    public static List<HiveSourceSplit> createInputSplits(
            int minNumSplits,
            List<HiveTablePartition> partitions,
            JobConf jobConf,
            ReadableConfig flinkConf)
            throws IOException {
        if (minNumSplits == 0) {
            // it's for infer parallelism
            LOGGER.info(
                    "The minNumSplits is {}, try to infer parallelism, set it to default parallelism {}.",
                    minNumSplits,
                    flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM));
            minNumSplits =
                    flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
        }
        setSplitMaxSize(partitions, jobConf, minNumSplits);
        int threadNum = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM);
        List<HiveSourceSplit> hiveSplits = new ArrayList<>();
        try (MRSplitsGetter splitsGetter = new MRSplitsGetter(threadNum)) {
            for (HiveTablePartitionSplits partitionSplits :
                    splitsGetter.getHiveTablePartitionMRSplits(minNumSplits, partitions, jobConf)) {
                HiveTablePartition partition = partitionSplits.getHiveTablePartition();
                for (InputSplit inputSplit : partitionSplits.getInputSplits()) {
                    Preconditions.checkState(
                            inputSplit instanceof FileSplit,
                            "Unsupported InputSplit type: " + inputSplit.getClass().getName());
                    hiveSplits.add(new HiveSourceSplit((FileSplit) inputSplit, partition, null));
                }
            }
        }
        LOGGER.info(
                "Create total input splits: {}. minNumSplits: {}", hiveSplits.size(), minNumSplits);
        return hiveSplits;
    }

    private static void setSplitMaxSize(
            List<HiveTablePartition> partitions, JobConf jobConf, int minNumSplits)
            throws IOException {
        long defaultMaxSplitBytes = 128 * 1024 * 1024; // 128M
        long openCost = 4 * 1024 * 1024; // 4M
        long totalByteWithOpenCost = calculateFilesSizeWithOpenCost(partitions, jobConf, openCost);
        long maxSplitBytes =
                calculateMaxSplitBytes(
                        totalByteWithOpenCost, minNumSplits, defaultMaxSplitBytes, openCost);
        LOGGER.info("The max split bytes is {}", maxSplitBytes);
        jobConf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(maxSplitBytes));
    }

    public static InputSplit[] createMRSplits(
            int minNumSplits, StorageDescriptor sd, JobConf jobConf) throws IOException {
        org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
        FileSystem fs = inputPath.getFileSystem(jobConf);
        // it's possible a partition exists in metastore but the data has been removed
        if (!fs.exists(inputPath)) {
            return new InputSplit[0];
        }
        InputFormat format;
        try {
            format =
                    (InputFormat)
                            Class.forName(
                                            sd.getInputFormat(),
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
        }
        ReflectionUtils.setConf(format, jobConf);
        // need to escape comma in the location path
        jobConf.set(FileInputFormat.INPUT_DIR, StringUtils.escapeString(sd.getLocation()));
        LOGGER.info("InputFormat: {}", format);
        // TODO: we should consider how to calculate the splits according to minNumSplits in the
        // future.
        return format.getSplits(jobConf, minNumSplits);
    }

    private static long calculateMaxSplitBytes(
            long totalBytesWithWeight,
            int minNumSplits,
            long defaultMaxSplitBytes,
            long openCostInBytes) {
        long bytesPerCore = totalBytesWithWeight / minNumSplits;
        return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
    }

    private static long calculateFilesSizeWithOpenCost(
            List<HiveTablePartition> partitions, JobConf jobConf, long openCost)
            throws IOException {
        long currentTimeMillis = System.currentTimeMillis();
        LOGGER.info("Start to calculate total fileSize");
        long totalBytesWithWeight = 0;
        long totalBytes = 0;
        for (HiveTablePartition partition : partitions) {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                continue;
            }
            for (FileStatus fileStatus : fs.listStatus(inputPath)) {
                long fileByte = fileStatus.getLen();
                totalBytes += fileByte;
                totalBytesWithWeight += (fileByte + openCost);
            }
        }
        LOGGER.info(
                "Cost {} ms to calculate total fileSize, the totalBytes is {}, the totalBytes with weights is {}.",
                System.currentTimeMillis() - currentTimeMillis,
                totalBytes,
                totalBytesWithWeight);
        return totalBytesWithWeight;
    }

    public static int getNumFiles(List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        int numFiles = 0;
        for (HiveTablePartition partition : partitions) {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                continue;
            }
            numFiles += fs.listStatus(inputPath).length;
        }
        return numFiles;
    }

    /** A factory to create {@link HiveSourceFileEnumerator}. */
    public static class Provider implements FileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final List<HiveTablePartition> partitions;
        private final JobConfWrapper jobConfWrapper;
        private final ReadableConfig flinkConf;

        public Provider(
                List<HiveTablePartition> partitions,
                JobConfWrapper jobConfWrapper,
                ReadableConfig flinkConf) {
            this.partitions = partitions;
            this.jobConfWrapper = jobConfWrapper;
            this.flinkConf = flinkConf;
        }

        @Override
        public FileEnumerator create() {
            return new HiveSourceFileEnumerator(partitions, jobConfWrapper.conf(), flinkConf);
        }
    }
}
