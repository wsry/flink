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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;

/** Unit tests for {@link DefaultResultPartition}. */
public class DefaultResultPartitionTest extends TestLogger {

    private static final TestResultPartitionStateSupplier resultPartitionState =
            new TestResultPartitionStateSupplier();

    private final IntermediateResultPartitionID resultPartitionId =
            new IntermediateResultPartitionID();
    private final IntermediateDataSetID intermediateResultId = new IntermediateDataSetID();

    private DefaultResultPartition resultPartition;

    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>>
            consumerVertexGroups = new HashMap<>();

    @BeforeEach
    void setUp() {
        resultPartition =
                new DefaultResultPartition(
                        resultPartitionId,
                        intermediateResultId,
                        BLOCKING,
                        resultPartitionState,
                        () ->
                                consumerVertexGroups.computeIfAbsent(
                                        resultPartitionId, ignored -> new ArrayList<>()),
                        () -> {
                            throw new UnsupportedOperationException();
                        });
    }

    @Test
    void testGetPartitionState() {
        for (ResultPartitionState state : ResultPartitionState.values()) {
            resultPartitionState.setResultPartitionState(state);
            Assertions.assertThat(resultPartition.getState()).isEqualTo(state);
        }
    }

    @Test
    void testGetConsumerVertexGroup() {

        Assertions.assertThat(resultPartition.getConsumerVertexGroups().isEmpty()).isTrue();

        // test update consumers
        ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        consumerVertexGroups.put(
                resultPartition.getId(),
                Collections.singletonList(ConsumerVertexGroup.fromSingleVertex(executionVertexId)));
        Assertions.assertThat(resultPartition.getConsumerVertexGroups().isEmpty()).isFalse();
        Assertions.assertThat(resultPartition.getConsumerVertexGroups().get(0))
                .contains(executionVertexId);
    }

    /** A test {@link ResultPartitionState} supplier. */
    private static class TestResultPartitionStateSupplier
            implements Supplier<ResultPartitionState> {

        private ResultPartitionState resultPartitionState;

        void setResultPartitionState(ResultPartitionState state) {
            resultPartitionState = state;
        }

        @Override
        public ResultPartitionState get() {
            return resultPartitionState;
        }
    }
}
