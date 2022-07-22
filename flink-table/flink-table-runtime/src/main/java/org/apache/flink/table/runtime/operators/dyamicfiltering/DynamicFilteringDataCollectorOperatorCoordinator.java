/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.dyamicfiltering;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** DynamicFilteringDataCollectorOperatorCoordinator. */
public class DynamicFilteringDataCollectorOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {

    private final CoordinatorStore coordinatorStore;
    private final List<String> dynamicFilteringDataListenerIDs;

    private boolean hasReceivedFilteringData;

    public DynamicFilteringDataCollectorOperatorCoordinator(
            Context context, List<String> dynamicFilteringDataListenerIDs) {
        this.coordinatorStore = context.getCoordinatorStore();
        this.dynamicFilteringDataListenerIDs = dynamicFilteringDataListenerIDs;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        // Since there might be speculative execution, once the dynamic filter collectors operator
        // has been executed for multiple attempts, we only keep the first notification.
        if (hasReceivedFilteringData) {
            return;
        }

        for (String listenerID : dynamicFilteringDataListenerIDs) {
            // push event
            OperatorCoordinator listener = (OperatorCoordinator) coordinatorStore.get(listenerID);
            if (listener == null) {
                throw new IllegalStateException("Dynamic filtering data listener missing");
            }
            listener.handleEventFromOperator(0, attemptNumber, event);
        }

        hasReceivedFilteringData = true;
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable reason) {}

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {}

    /** Provider for {@link DynamicFilteringDataCollectorOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorID;
        private final List<String> dynamicFilteringDataListenerIDs;

        public Provider(OperatorID operatorID, List<String> dynamicFilteringDataListenerIDs) {
            this.operatorID = operatorID;
            this.dynamicFilteringDataListenerIDs = dynamicFilteringDataListenerIDs;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new DynamicFilteringDataCollectorOperatorCoordinator(
                    context, dynamicFilteringDataListenerIDs);
        }
    }
}
