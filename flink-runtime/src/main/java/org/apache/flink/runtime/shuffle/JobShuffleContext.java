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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Job level shuffle context which can offer some job information like configuration, job ID and all
 * result partitions being tracked. Besides, one can stop tracking the lost result partitions.
 */
public interface JobShuffleContext {

    /** Returns the corresponding job configuration. */
    Configuration getConfiguration();

    /** Returns the corresponding {@link JobID}. */
    JobID getJobID();

    /**
     * Stops tracking the target result partitions, which means these partitions will be removed and
     * will be reproduced if used afterwards.
     */
    CompletableFuture<?> stopTrackingPartitions(Collection<ResultPartitionID> partitionIDS);

    /** Returns information of all partitions being tracked for the current job. */
    CompletableFuture<List<ResultPartitionDeploymentDescriptor>> listPartitions();
}
