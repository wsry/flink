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

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connectors.hive.HiveSourceFileEnumerator.createInputSplits;

/** HiveSourceDynamicFileEnumerator. */
public class HiveSourceDynamicFileEnumerator implements DynamicFileEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveSourceDynamicFileEnumerator.class);

    private final String table;
    private final List<String> dynamicPartitionKeys;
    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> allPartitions;
    private final JobConf jobConf;
    private transient List<HiveTablePartition> finalPartitions;

    public HiveSourceDynamicFileEnumerator(
            String table,
            List<String> dynamicPartitionKeys,
            List<HiveTablePartition> allPartitions,
            JobConf jobConf) {
        this.table = table;
        this.dynamicPartitionKeys = dynamicPartitionKeys;
        this.allPartitions = allPartitions;
        this.finalPartitions = allPartitions;
        this.jobConf = jobConf;
    }

    public void setDynamicFilteringData(DynamicFilteringData data) {
        LOG.info("Table: {}, DynamicFilteringData: {}", table, data);
        try {
            finalPartitions = new ArrayList<>();
            Optional<List<RowData>> receivedDataOpt = data.getData();
            if (!receivedDataOpt.isPresent()) {
                finalPartitions = allPartitions;
                return;
            }
            RowType rowType = data.getRowType();
            for (HiveTablePartition partition : allPartitions) {
                RowData rowData = createRowData(rowType, partition);
                if (data.contains(rowData)) {
                    finalPartitions.add(partition);
                }
            }
            LOG.info(
                    "Table: {}, Original partition number: {}, Remaining partition number: {}",
                    table,
                    allPartitions.size(),
                    finalPartitions.size());
        } catch (Exception e) {
            LOG.error("Failed to set partition data, will use all partitions", e);
            finalPartitions = allPartitions;
        }
    }

    private RowData createRowData(RowType rowType, HiveTablePartition partition) {
        Preconditions.checkArgument(rowType.getFieldCount() == dynamicPartitionKeys.size());
        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            String value = partition.getPartitionSpec().get(dynamicPartitionKeys.get(i));
            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case INTEGER:
                    rowData.setField(i, Integer.valueOf(value));
                    break;
                case BIGINT:
                    rowData.setField(i, Long.valueOf(value));
                    break;
                case VARCHAR:
                    rowData.setField(i, StringData.fromString(value));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return rowData;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(createInputSplits(minDesiredSplits, finalPartitions, jobConf));
    }

    /** A factory to create {@link HiveSourceDynamicFileEnumerator}. */
    public static class Provider implements DynamicFileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final String table;
        private final List<String> dynamicPartitionKeys;
        private final List<HiveTablePartition> partitions;
        private final JobConfWrapper jobConfWrapper;

        public Provider(
                String table,
                List<String> dynamicPartitionKeys,
                List<HiveTablePartition> partitions,
                JobConfWrapper jobConfWrapper) {
            this.table = table;
            this.dynamicPartitionKeys = dynamicPartitionKeys;
            this.partitions = partitions;
            this.jobConfWrapper = jobConfWrapper;
        }

        @Override
        public DynamicFileEnumerator create() {
            return new HiveSourceDynamicFileEnumerator(
                    table, dynamicPartitionKeys, partitions, jobConfWrapper.conf());
        }
    }
}
