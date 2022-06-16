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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 *
 * <p>Intermediate data sets may be read by other operators, materialized, or discarded.
 */
public class IntermediateDataSet implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final IntermediateDataSetID id; // the identifier

    private final JobVertex producer; // the operation that produced this data set

    // All consumers must have the same partitioner and parallelism
    private final List<JobEdge> consumers = new ArrayList<>();

    // The type of partition to use at runtime
    private final ResultPartitionType resultType;

    private boolean isBroadcast;

    private DistributionPattern distributionPattern;

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSet(
            IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);
        this.resultType = checkNotNull(resultType);
    }

    // --------------------------------------------------------------------------------------------

    public IntermediateDataSetID getId() {
        return id;
    }

    public JobVertex getProducer() {
        return producer;
    }

    public List<JobEdge> getConsumers() {
        return this.consumers;
    }

    @Nullable
    public JobEdge getConsumer() {
        if (this.consumers.isEmpty()) {
            return null;
        }
        return this.consumers.get(0);
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public DistributionPattern getDistributionPattern() {
        return checkNotNull(distributionPattern);
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    // --------------------------------------------------------------------------------------------

    public void addConsumer(JobEdge edge) {
        checkArgument(id.equals(edge.getSourceId()));

        if (consumers.isEmpty()) {
            distributionPattern = edge.getDistributionPattern();
            isBroadcast = edge.isBroadcast();
        } else {
            checkArgument(distributionPattern.equals(edge.getDistributionPattern()));
            checkArgument(isBroadcast == edge.isBroadcast());
        }
        consumers.add(edge);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Intermediate Data Set (" + id + ")";
    }
}
