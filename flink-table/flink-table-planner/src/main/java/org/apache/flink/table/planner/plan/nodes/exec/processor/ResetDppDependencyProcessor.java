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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * In the physical layer, we make dpp as the input of the fact source scan sot that the dpp nodes
 * would only be compiled once. But it causes a lot of issues during optimizing the exec graph.
 *
 * <p>In this processor, we'll remove the explicit inputs between dpp and the table source. Instead,
 * we'll bookkeep the dpp source inside the source exec node. Then at the last step, we'll analyse
 * if the dependency is required.
 */
public class ResetDppDependencyProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        AbstractExecNodeExactlyOnceVisitor dppSourceScanner =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        if (node instanceof BatchExecTableSourceScan
                                && node.getInputEdges().size() > 0) {
                            checkState(
                                    node.getInputEdges().size() == 1,
                                    "it should only have the dpp input");

                            ((BatchExecTableSourceScan) node)
                                    .setCachedDynamicFilteringDataCollector(
                                            (BatchExecDynamicFilteringDataCollector)
                                                    node.getInputEdges().get(0).getSource());
                            node.setInputEdges(Collections.emptyList());
                            node.setInputProperties(Collections.emptyList());
                        } else {
                            visitInputs(node);
                        }
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(dppSourceScanner));

        return execGraph;
    }
}
