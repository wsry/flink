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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSchedulerAndDeploy;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.transitionTasksToFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests that blocking result partitions are properly released. */
public class BlockingResultPartitionReleaseTest {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private ScheduledExecutorService scheduledExecutorService;
    private ComponentMainThreadExecutor mainThreadExecutor;
    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    @Before
    public void setup() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);
        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
    }

    @After
    public void teardown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    public void testMultipleConsumersForAdaptiveScheduler() throws Exception {
        testResultPartitionConsumedByMultiConsumers(true);
    }

    @Test
    public void testMultipleConsumersForDefaultScheduler() throws Exception {
        testResultPartitionConsumedByMultiConsumers(false);
    }

    private void testResultPartitionConsumedByMultiConsumers(boolean isAdaptive) throws Exception {
        int parallelism = 2;
        JobID jobId = new JobID();
        JobVertex producer = ExecutionGraphTestUtils.createNoOpVertex("producer", parallelism);
        JobVertex consumer1 = ExecutionGraphTestUtils.createNoOpVertex("consumer1", parallelism);
        JobVertex consumer2 = ExecutionGraphTestUtils.createNoOpVertex("consumer2", parallelism);

        TestingPartitionTracker partitionTracker = new TestingPartitionTracker();
        SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        isAdaptive,
                        jobId,
                        producer,
                        new JobVertex[] {consumer1, consumer2},
                        DistributionPattern.ALL_TO_ALL,
                        new TestingBlobWriter(Integer.MAX_VALUE),
                        mainThreadExecutor,
                        ioExecutor,
                        partitionTracker,
                        EXECUTOR_RESOURCE);
        ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        assertTrue(partitionTracker.releasedPartitions.isEmpty());

        CompletableFuture.runAsync(
                        () -> transitionTasksToFinished(executionGraph, consumer1.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        assertTrue(partitionTracker.releasedPartitions.isEmpty());

        CompletableFuture.runAsync(
                        () -> transitionTasksToFinished(executionGraph, consumer2.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        assertEquals(parallelism, partitionTracker.releasedPartitions.size());
        for (int i = 0; i < parallelism; ++i) {
            ExecutionJobVertex ejv = checkNotNull(executionGraph.getJobVertex(producer.getID()));
            assertEquals(
                    2,
                    ejv.getGraph()
                            .getEdgeManager()
                            .getConsumedPartitionGroupsById(
                                    partitionTracker.releasedPartitions.get(i).getPartitionId())
                            .size());
        }
    }

    private static class TestingPartitionTracker extends NoOpJobMasterPartitionTracker {

        private final List<ResultPartitionID> releasedPartitions = new ArrayList<>();

        @Override
        public void stopTrackingAndReleasePartitions(
                Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {
            releasedPartitions.addAll(checkNotNull(resultPartitionIds));
        }
    }
}
