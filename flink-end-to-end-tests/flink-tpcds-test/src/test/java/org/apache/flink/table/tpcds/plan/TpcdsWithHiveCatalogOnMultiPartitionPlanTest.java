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

package org.apache.flink.table.tpcds.plan;

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogWrapper;

import org.junit.Test;

import java.util.List;

public class TpcdsWithHiveCatalogOnMultiPartitionPlanTest extends TpcdsPlanTest {
    private static String scale = "10000";
    private static String multiPartition_database = "tpcds_bin_partitioned_orc_" + scale;
    private static String database = "tpcds_bin_orc_" + scale;
    private static final String HIVE_VERSION = "3.1.1";
    private static final String HIVE_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource(".").getPath();

    @Test
    public void getExecPlan() {
        HiveCatalogWrapper catalogWrapper =
                new HiveCatalogWrapper(
                        "hive", multiPartition_database, HIVE_CONF_DIR, HIVE_VERSION);
        tEnv.registerCatalog("hive", catalogWrapper);
        tEnv.useCatalog("hive");
        //        HiveCatalog catalog =
        //                new HiveCatalog("hive", multiPartition_database, HIVE_CONF_DIR,
        // HIVE_VERSION);
        //        tEnv.registerCatalog("hive", catalog);
        //        tEnv.useCatalog("hive");

        String sql = getSqlFile(caseName);
        util.verifyExecPlan(sql);
    }

    @Test
    public void getExplain() {
        HiveCatalogWrapper catalogWrapper =
                new HiveCatalogWrapper(
                        "hive", multiPartition_database, HIVE_CONF_DIR, HIVE_VERSION);
        tEnv.registerCatalog("hive", catalogWrapper);
        tEnv.useCatalog("hive");
        //        HiveCatalog catalog =
        //                new HiveCatalog("hive", multiPartition_database, HIVE_CONF_DIR,
        // HIVE_VERSION);
        //        tEnv.registerCatalog("hive", catalog);
        //        tEnv.useCatalog("hive");

        String sql = getSqlFile(caseName);
        util.verifyExplain(sql);
    }

    @Test
    public void testConnectToHive() {
        HiveCatalog catalog =
                new HiveCatalog("hive", multiPartition_database, HIVE_CONF_DIR, HIVE_VERSION);
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .ifPresent(
                        catalog1 -> {
                            try {
                                List<String> strings = catalog1.listTables(database);
                                for (String str : strings) {
                                    System.out.println(str);
                                }
                            } catch (DatabaseNotExistException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
