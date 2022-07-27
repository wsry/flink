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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactoryTest.deserializeShuffleDescriptors;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSchedulerAndDeploy;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.transitionTaskToFinished;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.transitionTasksToFinished;

/**
 * Tests for removing cached {@link ShuffleDescriptor}s when the related partitions are no longer
 * valid. Currently, there are two scenarios as illustrated in {@link
 * IntermediateResult#clearCachedInformationForPartitionGroup}.
 */
public class RemoveCachedShuffleDescriptorTest extends TestLogger {

    private static final int PARALLELISM = 4;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ScheduledExecutorService scheduledExecutorService;
    private ComponentMainThreadExecutor mainThreadExecutor;
    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    @BeforeEach
    void setup() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);
        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
    }

    @AfterEach
    void teardown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    void testRemoveNonOffloadedCacheForAllToAllEdgeAfterFinished() throws Exception {
        // Here we expect no offloaded BLOB.
        testRemoveCacheForAllToAllEdgeAfterFinished(new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    void testRemoveOffloadedCacheForAllToAllEdgeAfterFinished() throws Exception {
        // Here we expect 4 offloaded BLOBs:
        // JobInformation (1) + TaskInformation (2) + Cache of ShuffleDescriptors for the ALL-TO-ALL
        // edge (1).
        // When the downstream tasks are finished, the cache for ShuffleDescriptors should be
        // removed.
        testRemoveCacheForAllToAllEdgeAfterFinished(new TestingBlobWriter(0), 4, 3);
    }

    private void testRemoveCacheForAllToAllEdgeAfterFinished(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = ExecutionGraphTestUtils.createNoOpVertex("v1", PARALLELISM);
        final JobVertex v2 = ExecutionGraphTestUtils.createNoOpVertex("v2", PARALLELISM);

        final SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        false,
                        jobId,
                        v1,
                        new JobVertex[] {v2},
                        DistributionPattern.ALL_TO_ALL,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        NoOpJobMasterPartitionTracker.INSTANCE,
                        EXECUTOR_RESOURCE.getExecutor());
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        Assertions.assertThat(shuffleDescriptors.length).isEqualTo(PARALLELISM);
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedBefore);

        // For the all-to-all edge, we transition all downstream tasks to finished
        CompletableFuture.runAsync(
                        () -> transitionTasksToFinished(executionGraph, v2.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        // Cache should be removed since partitions are released
        Assertions.assertThat(getConsumedCachedShuffleDescriptor(executionGraph, v2)).isNull();
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedAfter);
    }

    @Test
    void testRemoveNonOffloadedCacheForAllToAllEdgeAfterFailover() throws Exception {
        testRemoveCacheForAllToAllEdgeAfterFailover(new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    void testRemoveOffloadedCacheForAllToAllEdgeAfterFailover() throws Exception {
        // Here we expect 4 offloaded BLOBs:
        // JobInformation (1) + TaskInformation (2) + Cache of ShuffleDescriptors for the ALL-TO-ALL
        // edge (1).
        // When the failover occurs for upstream tasks, the cache for ShuffleDescriptors should be
        // removed.
        testRemoveCacheForAllToAllEdgeAfterFailover(new TestingBlobWriter(0), 4, 3);
    }

    private void testRemoveCacheForAllToAllEdgeAfterFailover(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = ExecutionGraphTestUtils.createNoOpVertex("v1", PARALLELISM);
        final JobVertex v2 = ExecutionGraphTestUtils.createNoOpVertex("v2", PARALLELISM);

        final SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        false,
                        jobId,
                        v1,
                        new JobVertex[] {v2},
                        DistributionPattern.ALL_TO_ALL,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        NoOpJobMasterPartitionTracker.INSTANCE,
                        EXECUTOR_RESOURCE.getExecutor());
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        Assertions.assertThat(shuffleDescriptors.length).isEqualTo(PARALLELISM);
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedBefore);

        triggerGlobalFailoverAndComplete(scheduler, v1);
        ioExecutor.triggerAll();

        // Cache should be removed during ExecutionVertex#resetForNewExecution
        Assertions.assertThat(getConsumedCachedShuffleDescriptor(executionGraph, v2)).isNull();
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedAfter);
    }

    @Test
    void testRemoveNonOffloadedCacheForPointwiseEdgeAfterFinished() throws Exception {
        testRemoveCacheForPointwiseEdgeAfterFinished(
                new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    void testRemoveOffloadedCacheForPointwiseEdgeAfterFinished() throws Exception {
        // Here we expect 7 offloaded BLOBs:
        // JobInformation (1) + TaskInformation (2) + Cache of ShuffleDescriptors for the POINTWISE
        // edges (4).
        // When the downstream tasks are finished, the cache for ShuffleDescriptors should be
        // removed.
        testRemoveCacheForPointwiseEdgeAfterFinished(new TestingBlobWriter(0), 7, 6);
    }

    private void testRemoveCacheForPointwiseEdgeAfterFinished(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = ExecutionGraphTestUtils.createNoOpVertex("v1", PARALLELISM);
        final JobVertex v2 = ExecutionGraphTestUtils.createNoOpVertex("v2", PARALLELISM);

        final SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        false,
                        jobId,
                        v1,
                        new JobVertex[] {v2},
                        DistributionPattern.POINTWISE,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        NoOpJobMasterPartitionTracker.INSTANCE,
                        EXECUTOR_RESOURCE.getExecutor());
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        Assertions.assertThat(shuffleDescriptors.length).isEqualTo(1);
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedBefore);

        // For the pointwise edge, we just transition the first downstream task to FINISHED
        ExecutionVertex ev21 =
                Objects.requireNonNull(executionGraph.getJobVertex(v2.getID()))
                        .getTaskVertices()[0];
        CompletableFuture.runAsync(
                        () -> transitionTaskToFinished(executionGraph, ev21), mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        // The cache of the first upstream task should be removed since its partition is released
        Assertions.assertThat(getConsumedCachedShuffleDescriptor(executionGraph, v2, 0)).isNull();

        // The cache of the other upstream tasks should stay
        final ShuffleDescriptor[] shuffleDescriptorsForOtherVertex =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2, 1),
                        jobId,
                        blobWriter);
        Assertions.assertThat(shuffleDescriptorsForOtherVertex.length).isEqualTo(1);

        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedAfter);
    }

    @Test
    void testRemoveNonOffloadedCacheForPointwiseEdgeAfterFailover() throws Exception {
        testRemoveCacheForPointwiseEdgeAfterFailover(
                new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    void testRemoveOffloadedCacheForPointwiseEdgeAfterFailover() throws Exception {
        // Here we expect 7 offloaded BLOBs:
        // JobInformation (1) + TaskInformation (2) + Cache of ShuffleDescriptors for the POINTWISE
        // edges (4).
        // When the failover occurs for upstream tasks, the cache for ShuffleDescriptors should be
        // removed.
        testRemoveCacheForPointwiseEdgeAfterFailover(new TestingBlobWriter(0), 7, 6);
    }

    private void testRemoveCacheForPointwiseEdgeAfterFailover(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = ExecutionGraphTestUtils.createNoOpVertex("v1", PARALLELISM);
        final JobVertex v2 = ExecutionGraphTestUtils.createNoOpVertex("v2", PARALLELISM);

        final SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        false,
                        jobId,
                        v1,
                        new JobVertex[] {v2},
                        DistributionPattern.POINTWISE,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        NoOpJobMasterPartitionTracker.INSTANCE,
                        EXECUTOR_RESOURCE.getExecutor());
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        Assertions.assertThat(shuffleDescriptors.length).isEqualTo(1);
        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedBefore);

        triggerExceptionAndComplete(executionGraph, v1, v2);
        ioExecutor.triggerAll();

        // The cache of the first upstream task should be removed during
        // ExecutionVertex#resetForNewExecution
        Assertions.assertThat(getConsumedCachedShuffleDescriptor(executionGraph, v2, 0)).isNull();

        // The cache of the other upstream tasks should stay
        final ShuffleDescriptor[] shuffleDescriptorsForOtherVertex =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2, 1),
                        jobId,
                        blobWriter);
        Assertions.assertThat(shuffleDescriptorsForOtherVertex.length).isEqualTo(1);

        Assertions.assertThat(blobWriter.numberOfBlobs()).isEqualTo(expectedAfter);
    }

    private void triggerGlobalFailoverAndComplete(SchedulerBase scheduler, JobVertex upstream)
            throws TimeoutException {

        final Throwable t = new Exception();
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        CompletableFuture.runAsync(
                        () -> {
                            // Trigger a failover
                            scheduler.handleGlobalFailure(t);
                            // Finish the cancellation of downstream tasks and restart tasks
                            for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
                                ev.getCurrentExecutionAttempt().completeCancelling();
                            }
                        },
                        mainThreadExecutor)
                .join();

        // Wait until all the upstream vertices finish restarting
        for (ExecutionVertex ev :
                Objects.requireNonNull(executionGraph.getJobVertex(upstream.getID()))
                        .getTaskVertices()) {
            ExecutionGraphTestUtils.waitUntilExecutionVertexState(
                    ev, ExecutionState.DEPLOYING, 1000);
        }
    }

    private void triggerExceptionAndComplete(
            ExecutionGraph executionGraph, JobVertex upstream, JobVertex downstream)
            throws TimeoutException {

        final ExecutionVertex ev11 =
                Objects.requireNonNull(executionGraph.getJobVertex(upstream.getID()))
                        .getTaskVertices()[0];
        final ExecutionVertex ev21 =
                Objects.requireNonNull(executionGraph.getJobVertex(downstream.getID()))
                        .getTaskVertices()[0];

        CompletableFuture.runAsync(
                        () -> {
                            // Trigger a PartitionNotFoundException for downstream tasks
                            ev21.markFailed(
                                    new PartitionNotFoundException(new ResultPartitionID()));
                        },
                        mainThreadExecutor)
                .join();
        ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev11, ExecutionState.DEPLOYING, 1000);
    }

    // ============== Utils ==============

    private static MaybeOffloaded<ShuffleDescriptor[]> getConsumedCachedShuffleDescriptor(
            ExecutionGraph executionGraph, JobVertex vertex) {
        return getConsumedCachedShuffleDescriptor(executionGraph, vertex, 0);
    }

    private static MaybeOffloaded<ShuffleDescriptor[]> getConsumedCachedShuffleDescriptor(
            ExecutionGraph executionGraph, JobVertex vertex, int taskNum) {

        final ExecutionJobVertex ejv = executionGraph.getJobVertex(vertex.getID());
        final List<IntermediateResult> consumedResults = Objects.requireNonNull(ejv).getInputs();
        final IntermediateResult consumedResult = consumedResults.get(0);

        return consumedResult.getCachedShuffleDescriptors(
                ejv.getTaskVertices()[taskNum].getConsumedPartitionGroup(0));
    }
}
