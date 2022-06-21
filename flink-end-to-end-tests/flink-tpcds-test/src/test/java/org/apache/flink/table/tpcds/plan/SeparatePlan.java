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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class SeparatePlan {
    public static void main(String[] args) throws IOException, URISyntaxException {
        String filePath =
                "org/apache/flink/table/tpcds/plan/TpcdsWithHiveCatalogOnMultiPartitionPlanTest.xml";
        String path = SeparatePlan.class.getResource("/" + filePath).getPath();
        File file = new File(path);
        List<String> lines = Files.readAllLines(file.toPath());
        List<String> items = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith("  <TestCase name=\"getExecPlan[") && line.endsWith("]\">")) {
                if (!items.isEmpty()) {
                    String name = items.get(0).substring(line.indexOf('[') + 1, line.indexOf(']'));
                    toNewFile(name, items);
                    items.clear();
                }
            }
            items.add(line);
        }
    }

    public static void toNewFile(String name, List<String> items) throws IOException {
        String path =
                "/Users/zhengyunhong/workspace/flink/flink-end-to-end-tests/flink-tpcds-test/src/test/resources/org/apache/flink/table/tpcds/plan/"
                        + name
                        + ".xml";
        File file = new File(path);
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (String l : items) {
            writer.write(l + "\r\n");
        }
        writer.close();
    }
}
