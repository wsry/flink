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

package org.apache.flink.runtime.rpc.serializer;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.SerializedValue;

import org.nustaq.serialization.FSTConfiguration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;

/**
 * A FST-based serializer (see https://github.com/RuedigerMoeller/fast-serialization)
 * for flink rpc message serialization.
 */
public class FSTBasedRpcMessageSerializer {

	private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
		FSTConfiguration configuration = FSTConfiguration.createDefaultConfiguration();
		configuration.registerClass(
			TaskDeploymentDescriptor.class,
			ResultPartitionDeploymentDescriptor.class,
			InputGateDeploymentDescriptor.class,
			PartitionDescriptor.class,
			NettyShuffleDescriptor.class,
			IntermediateDataSetID.class,
			IntermediateResultPartitionID.class,
			ResultPartitionType.class,
			SerializedValue.class,
			TaskDeploymentDescriptor.MaybeOffloaded.class,
			TaskDeploymentDescriptor.NonOffloaded.class,
			JobVertexID.class,
			PermanentBlobKey.class,
			JobInformation.class,
			TaskInformation.class,
			Configuration.class,
			PartitionInfo.class,
			Acknowledge.class,
			SerializedInputSplit.class,
			SlotOffer.class,
			SlotReport.class,
			SlotStatus.class,
			SlotID.class,
			ResourceProfile.class,
			NettyShuffleDescriptor.NetworkPartitionConnectionInfo.class,
			NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo.class,
			JobID.class,
			AllocationID.class,
			ResultPartitionID.class,
			ResourceID.class,
			ExecutionAttemptID.class,
			ResourceManagerId.class,
			SlotRequest.class,
			JobMasterId.class,
			InstanceID.class,
			HardwareDescription.class,
			ConnectionID.class,
			InetSocketAddress.class,
			InetAddress.class,
			URL.class,
			ApplicationStatus.class,
			IOMetrics.class,
			ExecutionState.class,
			CheckpointOptions.class,
			CheckpointType.class,
			CheckpointStorageLocationReference.class,
			JobManagerTaskRestore.class,
			TaskStateSnapshot.class,
			OperatorID.class,
			OperatorSubtaskState.class,
			StateObjectCollection.class,
			OperatorStreamStateHandle.class,
			IncrementalLocalKeyedStateHandle.class,
			IncrementalRemoteKeyedStateHandle.class,
			StateHandleID.class,
			KeyGroupsStateHandle.class,
			KeyGroupRangeOffsets.class,
			KeyGroupRange.class,
			KvStateID.class,
			CheckpointMetrics.class,
			DeclineCheckpoint.class,
			TaskManagerLocation.class,
			TaskManagerInfo.class,
			ResourceOverview.class,
			AccumulatorReport.class,
			AccumulatorSnapshot.class,
			JobDetails.class,
			JobStatus.class,
			ArchivedExecutionGraph.class,
			ArchivedExecutionJobVertex.class,
			StringifiedAccumulatorResult.class,
			ArchivedExecutionVertex.class,
			ArchivedExecution.class,
			EvictingBoundedList.class,
			ArchivedExecutionConfig.class,
			CheckpointStatsSnapshot.class,
			CheckpointCoordinatorConfiguration.class,
			ErrorInfo.class,
			OperatorBackPressureStats.class,
			OperatorBackPressureStatsResponse.class);
		return configuration;
	});

	public static byte[] serialize(Object object) {
		return conf.get().asByteArray(object);
	}

	@SuppressWarnings("unchecked")
	public static  <T> T deserialize(byte[] bytes) {
		return (T) conf.get().asObject(bytes);
	}
}
