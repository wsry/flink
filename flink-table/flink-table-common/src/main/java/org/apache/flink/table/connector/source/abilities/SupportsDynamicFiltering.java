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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.List;

/**
 * Push dynamic filter into {@link ScanTableSource}, the table source can filter the partitions or
 * the input data in runtime to reduce scan I/O.
 */
public interface SupportsDynamicFiltering {

    /**
     * apply the candidate filter fields into the table source, and return the accepted fields. The
     * data corresponding the filter fields will be provided in runtime, which can be used to filter
     * partitions and input data.
     */
    List<String> applyDynamicFiltering(List<String> candidateFilterFields);
}
