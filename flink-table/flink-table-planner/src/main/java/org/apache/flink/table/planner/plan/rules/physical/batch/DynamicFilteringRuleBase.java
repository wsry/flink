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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED;

/** DynamicFilteringRuleBase. */
public abstract class DynamicFilteringRuleBase extends RelRule<RelRule.Config> {

    public DynamicFilteringRuleBase(RelRule.Config config) {
        super(config);
    }

    public boolean doMatches(
            BatchPhysicalJoinBase join,
            BatchPhysicalRel dimSide,
            BatchPhysicalTableSourceScan factScan,
            boolean factInLeft) {
        if (!ShortcutUtils.unwrapContext(join)
                .getTableConfig()
                .get(TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return false;
        }
        if (factScan instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
            // rule applied
            return false;
        }

        // TODO support more join types
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }

        if (!isDimSide(dimSide)) {
            return false;
        }

        // TODO Only support FLIP-27 source
        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return false;
        }
        CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getTable();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            return false;
        }
        DynamicTableSource tableSource = tableSourceTable.tableSource();
        if (!(tableSource instanceof SupportsDynamicFiltering)) {
            return false;
        }

        List<Integer> factJoinKeys = factInLeft ? joinInfo.leftKeys : joinInfo.rightKeys;

        List<String> candidateFields =
                factJoinKeys.stream()
                        .map(i -> factScan.getRowType().getFieldNames().get(i))
                        .collect(Collectors.toList());
        List<String> acceptedFields =
                ((SupportsDynamicFiltering) tableSource).applyDynamicFiltering(candidateFields);
        return !acceptedFields.isEmpty();
    }

    private static boolean isDimSide(RelNode rel) {
        DppDimSideFactors dimSideFactors = new DppDimSideFactors();
        visitDimSide(rel, dimSideFactors);
        return dimSideFactors.isDimSide();
    }

    private static void visitDimSide(RelNode rel, DppDimSideFactors dimSideFactors) {
        if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return;
            }
            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            dimSideFactors.hasNonPartitionedScan = !catalogTable.isPartitioned();
        } else if (rel instanceof HepRelVertex) {
            visitDimSide(((HepRelVertex) rel).getCurrentRel(), dimSideFactors);
        } else if (rel instanceof Exchange || rel instanceof Project) {
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Calc) {
            if (((Calc) rel).getProgram().getCondition() != null) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Filter && ((Filter) rel).getCondition() != null) {
            dimSideFactors.hasFilter = true;
            visitDimSide(rel.getInput(0), dimSideFactors);
        }
    }

    private static class DppDimSideFactors {
        private boolean hasFilter;
        private boolean hasNonPartitionedScan;

        public boolean isDimSide() {
            return hasFilter && hasNonPartitionedScan;
        }
    }

    protected BatchPhysicalDynamicFilteringTableSourceScan createDynamicFilteringTableSourceScan(
            BatchPhysicalTableSourceScan factScan,
            BatchPhysicalRel dimSide,
            BatchPhysicalJoinBase join,
            boolean factInLeft) {
        JoinInfo joinInfo = join.analyzeCondition();
        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource tableSource = tableSourceTable.tableSource();

        List<Integer> factJoinKeys = factInLeft ? joinInfo.leftKeys : joinInfo.rightKeys;
        List<Integer> dimJoinKeys = factInLeft ? joinInfo.rightKeys : joinInfo.leftKeys;
        List<String> candidateFields =
                factJoinKeys.stream()
                        .map(i -> factScan.getRowType().getFieldNames().get(i))
                        .collect(Collectors.toList());
        List<String> acceptedFields =
                ((SupportsDynamicFiltering) tableSource).applyDynamicFiltering(candidateFields);
        if (acceptedFields.isEmpty()) {
            return null;
        }
        List<Integer> acceptedFieldIndices =
                acceptedFields.stream()
                        .map(f -> factScan.getRowType().getFieldNames().indexOf(f))
                        .collect(Collectors.toList());

        List<Integer> dynamicFilteringFieldIndices = new ArrayList<>();
        for (int i = 0; i < joinInfo.leftKeys.size(); ++i) {
            if (acceptedFieldIndices.contains(factJoinKeys.get(i))) {
                dynamicFilteringFieldIndices.add(dimJoinKeys.get(i));
            }
        }
        final BatchPhysicalDynamicFilteringDataCollector dppConnector =
                createDynamicFilteringConnector(dimSide, dynamicFilteringFieldIndices);
        return new BatchPhysicalDynamicFilteringTableSourceScan(
                factScan.getCluster(),
                factScan.getTraitSet(),
                factScan.getHints(),
                factScan.tableSourceTable(),
                dppConnector);
    }

    private BatchPhysicalDynamicFilteringDataCollector createDynamicFilteringConnector(
            RelNode dimSide, List<Integer> dynamicFilteringFieldIndices) {
        final RelDataType outputType =
                ((FlinkTypeFactory) dimSide.getCluster().getTypeFactory())
                        .projectStructType(
                                dimSide.getRowType(),
                                dynamicFilteringFieldIndices.stream().mapToInt(i -> i).toArray());

        return new BatchPhysicalDynamicFilteringDataCollector(
                dimSide.getCluster(),
                dimSide.getTraitSet(),
                ignoreExchange(dimSide),
                outputType,
                dynamicFilteringFieldIndices.stream().mapToInt(i -> i).toArray());
    }

    private RelNode ignoreExchange(RelNode dimSide) {
        if (dimSide instanceof Exchange) {
            return dimSide.getInput(0);
        } else {
            return dimSide;
        }
    }
}
