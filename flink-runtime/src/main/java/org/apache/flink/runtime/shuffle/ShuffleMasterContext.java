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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Shuffle context used to create {@link ShuffleMaster}. It can work as a proxy to other cluster
 * components and hide these components from users.
 */
public interface ShuffleMasterContext {

    /** Returns the cluster configuration. */
    Configuration getConfiguration();

    /** Handles the fatal error if any. */
    void onFatalError(Throwable throwable);

    /**
     * Stops tracking the target dataset (cluster partitions), which means these data can not be
     * reused anymore.
     */
    CompletableFuture<?> stopTrackingDataSet(IntermediateDataSetID dataSetID);

    /** Returns IDs of all datasets (cluster partitions) being tracked by this cluster currently. */
    CompletableFuture<List<IntermediateDataSetID>> listDataSets();
}
