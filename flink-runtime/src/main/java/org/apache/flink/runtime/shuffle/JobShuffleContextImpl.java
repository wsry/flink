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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of {@link JobShuffleContext}. */
public class JobShuffleContextImpl implements JobShuffleContext {

    private final Configuration configuration;

    private final JobID jobID;

    private final JobMasterPartitionTracker partitionTracker;

    private final ComponentMainThreadExecutor executor;

    public JobShuffleContextImpl(
            JobID jobID,
            Configuration configuration,
            JobMasterPartitionTracker partitionTracker,
            ComponentMainThreadExecutor executor) {
        this.jobID = checkNotNull(jobID);
        this.configuration = checkNotNull(configuration);
        this.partitionTracker = checkNotNull(partitionTracker);
        this.executor = checkNotNull(executor);
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public CompletableFuture<?> stopTrackingPartitions(Collection<ResultPartitionID> partitionIDS) {
        CompletableFuture<?> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        partitionTracker.stopTrackingAndReleasePartitions(partitionIDS);
                        future.complete(null);
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<List<ResultPartitionDeploymentDescriptor>> listPartitions() {
        CompletableFuture<List<ResultPartitionDeploymentDescriptor>> future =
                new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        List<ResultPartitionDeploymentDescriptor> partitions =
                                partitionTracker.listPartitions();
                        future.complete(partitions);
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                });
        return future;
    }
}
