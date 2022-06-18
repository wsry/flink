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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.util.HiveStatsUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveCatalogWrapper extends HiveCatalog {
    // partitionNames and its ColumnStatisticsObj
    private static Map<ObjectPath, Map<String, List<ColumnStatisticsObj>>>
            catalogColumnStatisticsCache = new HashMap<>();

    public HiveCatalogWrapper(
            String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir) {
        super(catalogName, defaultDatabase, hiveConfDir);
    }

    public HiveCatalogWrapper(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            String hiveVersion) {
        super(catalogName, defaultDatabase, hiveConfDir, hiveVersion);
    }

    public HiveCatalogWrapper(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable String hiveConfDir,
            @Nullable String hadoopConfDir,
            @Nullable String hiveVersion) {
        super(catalogName, defaultDatabase, hiveConfDir, hadoopConfDir, hiveVersion);
    }

    public HiveCatalogWrapper(
            String catalogName,
            @Nullable String defaultDatabase,
            @Nullable HiveConf hiveConf,
            @Nullable String hiveVersion) {
        super(catalogName, defaultDatabase, hiveConf, hiveVersion);
    }

    protected HiveCatalogWrapper(
            String catalogName,
            String defaultDatabase,
            @Nullable HiveConf hiveConf,
            String hiveVersion,
            boolean allowEmbedded) {
        super(catalogName, defaultDatabase, hiveConf, hiveVersion, allowEmbedded);
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        Table hiveTable = getHiveTable(tablePath);
        long startTime = System.currentTimeMillis();
        try {
            if (!isTablePartitioned(hiveTable)) {
                List<ColumnStatisticsObj> columnStatisticsObjs =
                        client.getTableColumnStatistics(
                                hiveTable.getDbName(),
                                hiveTable.getTableName(),
                                getFieldNames(hiveTable.getSd().getCols()));
                System.out.println(
                        String.format(
                                "Get table column statistics for table %s cost %s ms.",
                                hiveTable.getTableName(), System.currentTimeMillis() - startTime));
                return new CatalogColumnStatistics(
                        HiveStatsUtil.createCatalogColumnStats(columnStatisticsObjs, hiveVersion));
            } else {
                // to get column statistic, we merge the statistic of all
                // partitions for all columns
                // list all partitions
                List<String> partNames = new ArrayList<>();
                Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics = null;
                if (catalogColumnStatisticsCache.containsKey(tablePath)) {
                    partitionColumnStatistics = catalogColumnStatisticsCache.get(tablePath);
                    for (Map.Entry<String, List<ColumnStatisticsObj>> pStats :
                            partitionColumnStatistics.entrySet()) {
                        partNames.add(pStats.getKey());
                    }
                } else {
                    partNames =
                            client.listPartitionNames(
                                    hiveTable.getDbName(), hiveTable.getTableName(), (short) -1);
                    // get statistic from partition to all columns' statics
                    partitionColumnStatistics =
                            client.getPartitionColumnStatistics(
                                    hiveTable.getDbName(),
                                    hiveTable.getTableName(),
                                    partNames,
                                    getFieldNames(hiveTable.getSd().getCols()));
                    catalogColumnStatisticsCache.put(tablePath, partitionColumnStatistics);
                }

                // get statistic for partition column, it's hard code just for test purpose
                Long min = null;
                Long max = null;
                Long ndv = null;
                Long nullCount = null;
                String partitionCol = hiveTable.getPartitionKeys().get(0).getName();
                for (String partition : partNames) {
                    String[] partVal = partition.split("=");
                    String value = partVal[1];
                    if (value.equals("__HIVE_DEFAULT_PARTITION__")) {
                        ndv = HiveStatsUtil.add(ndv, 1L);
                        try {
                            nullCount =
                                    getPartitionStatistics(
                                                    tablePath,
                                                    new CatalogPartitionSpec(
                                                            Collections.singletonMap(
                                                                    partitionCol,
                                                                    "__HIVE_DEFAULT_PARTITION__")))
                                            .getRowCount();
                        } catch (Exception e) {
                            throw new Exception();
                        }
                    } else {
                        Long pv = Long.valueOf(value);
                        min = HiveStatsUtil.min(min, pv);
                        max = HiveStatsUtil.max(max, pv);
                        ndv = HiveStatsUtil.add(ndv, 1L);
                    }
                }
                CatalogColumnStatisticsDataLong partitionColumnStatisticsDataLong =
                        new CatalogColumnStatisticsDataLong(min, max, ndv, nullCount);
                Map<String, CatalogColumnStatisticsDataBase> nonPartitionColumnStatistic =
                        HiveStatsUtil.createCatalogColumnStats(
                                partitionColumnStatistics, hiveVersion, this, tablePath);
                nonPartitionColumnStatistic.put(partitionCol, partitionColumnStatisticsDataLong);
                CatalogColumnStatistics catalogColumnStatistics =
                        new CatalogColumnStatistics(nonPartitionColumnStatistic);
                System.out.println(
                        String.format(
                                "Get table column statistics for table %s cost %s ms.",
                                hiveTable.getTableName(), System.currentTimeMillis() - startTime));
                return catalogColumnStatistics;
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table column stats of table %s",
                            tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public Tuple2<CatalogTableStatistics, CatalogColumnStatistics> getPartitionTableStats(
            ObjectPath tablePath, List<Map<String, String>> remainingPartitions) throws Exception {
        Table hiveTable = getHiveTable(tablePath);
        // we accumulate the statistic for all partitions
        List<CatalogTableStatistics> partitionStatisticsList;
        List<String> partitionNames = new ArrayList<>();
        for (Map<String, String> partitionEntry : remainingPartitions) {
            List<String> partitionSpecs = new ArrayList<>();
            for (Map.Entry<String, String> p : partitionEntry.entrySet()) {
                partitionSpecs.add(p.getKey() + "=" + p.getValue());
            }
            partitionNames.add(String.join("/", partitionSpecs));
        }
        Set<String> remainingPartitionsNameSets = new HashSet<>(partitionNames);
        List<String> partitionKeys =
                hiveTable.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        try {
            partitionStatisticsList =
                    client
                            .listPartitions(
                                    tablePath.getDatabaseName(),
                                    tablePath.getObjectName(),
                                    (short) -1)
                            .stream()
                            .filter(
                                    p -> {
                                        String pName = getPartitionName(partitionKeys, p);
                                        return remainingPartitionsNameSets.contains(pName);
                                    })
                            .map(p -> createCatalogTableStatistics(p.getParameters()))
                            .collect(Collectors.toList());
        } catch (TException e) {
            throw new CatalogException("Fail to list partitions for table " + tablePath, e);
        }
        CatalogTableStatistics catalogTableStatistics =
                CatalogTableStatistics.accumulateStatistics(partitionStatisticsList);

        // get partition column statistics.
        Map<String, List<ColumnStatisticsObj>> partitionColumnStatisticsMap;
        if (!catalogColumnStatisticsCache.containsKey(tablePath)) {
            catalogColumnStatisticsCache.put(tablePath, new HashMap<>());
        }
        partitionColumnStatisticsMap = catalogColumnStatisticsCache.get(tablePath);
        Map<String, List<ColumnStatisticsObj>> oldPartitionColumnStatistics = null;
        List<String> newPartitionNames = new ArrayList<>();
        for (String partitionName : partitionNames) {
            if (partitionColumnStatisticsMap.containsKey(partitionName)) {
                oldPartitionColumnStatistics.put(
                        partitionName, partitionColumnStatisticsMap.get(partitionName));
            } else {
                newPartitionNames.add(partitionName);
            }
        }

        if (newPartitionNames.size() != 0) {
            Map<String, List<ColumnStatisticsObj>> newPartitionColumnStatistics =
                    client.getPartitionColumnStatistics(
                            tablePath.getDatabaseName(),
                            hiveTable.getTableName(),
                            newPartitionNames,
                            getFieldNames(hiveTable.getSd().getCols()));
            // insert to cache
            for (Map.Entry<String, List<ColumnStatisticsObj>> newPartitionColumnStat :
                    newPartitionColumnStatistics.entrySet()) {
                catalogColumnStatisticsCache
                        .get(tablePath)
                        .put(newPartitionColumnStat.getKey(), newPartitionColumnStat.getValue());
                oldPartitionColumnStatistics.put(
                        newPartitionColumnStat.getKey(), newPartitionColumnStat.getValue());
            }
        }

        // get statistic for partition column, it's hard code just for test purpose
        Long min = null;
        Long max = null;
        Long ndv = null;
        Long nullCount = null;
        String partitionCol = hiveTable.getPartitionKeys().get(0).getName();
        for (String partition : partitionNames) {
            String[] partVal = partition.split("=");
            String value = partVal[1];
            if (value.equals("__HIVE_DEFAULT_PARTITION__")) {
                ndv = HiveStatsUtil.add(ndv, 1L);
                try {
                    nullCount =
                            getPartitionStatistics(
                                            tablePath,
                                            new CatalogPartitionSpec(
                                                    Collections.singletonMap(
                                                            partitionCol,
                                                            "__HIVE_DEFAULT_PARTITION__")))
                                    .getRowCount();
                } catch (Exception e) {
                    throw new Exception();
                }
            } else {
                Long pv = Long.valueOf(value);
                min = HiveStatsUtil.min(min, pv);
                max = HiveStatsUtil.max(max, pv);
                ndv = HiveStatsUtil.add(ndv, 1L);
            }
        }
        CatalogColumnStatisticsDataLong partitionColumnStatisticsDataLong =
                new CatalogColumnStatisticsDataLong(min, max, ndv, nullCount);
        Map<String, CatalogColumnStatisticsDataBase> nonPartitionColumnStatistic =
                HiveStatsUtil.createCatalogColumnStats(
                        oldPartitionColumnStatistics, hiveVersion, this, tablePath);
        nonPartitionColumnStatistic.put(partitionCol, partitionColumnStatisticsDataLong);
        CatalogColumnStatistics catalogColumnStatistics =
                new CatalogColumnStatistics(nonPartitionColumnStatistic);

        // return convertToTableStats(catalogTableStatistics, catalogColumnStatistics);
        return Tuple2.of(catalogTableStatistics, catalogColumnStatistics);
    }
}
