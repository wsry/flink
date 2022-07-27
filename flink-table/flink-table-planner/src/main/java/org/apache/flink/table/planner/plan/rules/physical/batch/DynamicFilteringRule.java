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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

/** DynamicFilteringRule */
public class DynamicFilteringRule extends RelRule<RelRule.Config> {

    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED =
            key("table.optimizer.dynamic-filtering-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public DynamicFilteringRule(Config config) {
        super(config);
    }

    public static final RelOptRule INSTANCE =
            DynamicFilteringRule1.Config.EMPTY
                    .withDescription("DynamicFilteringRule")
                    .as(Config.class)
                    .rule()
                    .toRule();

    /** Config. */
    public interface Config extends RelRule.Config {
        @Override
        default DynamicFilteringRule toRule() {
            return new DynamicFilteringRule(this);
        }

        default Config rule() {
            return withOperandSupplier(o -> o.operand(BatchPhysicalJoinBase.class).anyInputs())
                    .as(Config.class);
        }
    }

    public static boolean supportDynamicFilter(Join join) {
        if (!ShortcutUtils.unwrapContext(join)
                .getTableConfig()
                .get(TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return false;
        }
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        ImmutableIntList leftPartitionKeys =
                extractPartitionKeysFromFactSide(left, joinInfo.leftKeys);

        if (!leftPartitionKeys.isEmpty()) {
            boolean rightIsDim =
                    isDimSide(
                            right,
                            getDimSidePartitionKeys(
                                    joinInfo.leftKeys, joinInfo.rightKeys, leftPartitionKeys));
            if (rightIsDim) {
                return true;
            }
        }

        ImmutableIntList rightPartitionKeys =
                extractPartitionKeysFromFactSide(right, joinInfo.rightKeys);
        if (!rightPartitionKeys.isEmpty()) {
            return isDimSide(
                    left,
                    getDimSidePartitionKeys(
                            joinInfo.rightKeys, joinInfo.leftKeys, rightPartitionKeys));
        }
        return false;
    }

    private static ImmutableIntList getDimSidePartitionKeys(
            ImmutableIntList factKeys,
            ImmutableIntList dimKeys,
            ImmutableIntList factPartitionKeys) {
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < factKeys.size(); ++i) {
            int k = factKeys.get(i);
            if (factPartitionKeys.contains(k)) {
                keys.add(dimKeys.get(i));
            }
        }
        return ImmutableIntList.copyOf(keys);
    }

    private static boolean isDimSide(RelNode rel, ImmutableIntList joinKeys) {
        return isDimSide0(rel, joinKeys) && hasFilter(rel);
    }

    private static boolean isDimSide0(RelNode rel, ImmutableIntList joinKeys) {
        if (rel instanceof HepRelVertex) {
            return isDimSide0(((HepRelVertex) rel).getCurrentRel(), joinKeys);
        } else if (rel instanceof Exchange || rel instanceof Filter) {
            return isDimSide0(rel.getInput(0), joinKeys);
        } else if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return false;
            }
            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            return !catalogTable.isPartitioned();
        } else if (rel instanceof Project) {
            List<RexNode> projects = ((Project) rel).getProjects();
            ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
            if (inputJoinKeys.isEmpty()) {
                return false;
            }
            return isDimSide0(rel.getInput(0), inputJoinKeys);
        } else if (rel instanceof Calc) {
            Calc calc = (Calc) rel;
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(p -> calc.getProgram().expandLocalRef(p))
                            .collect(Collectors.toList());
            ImmutableIntList inputJoinKeys = getInputIndices(projects, joinKeys);
            if (inputJoinKeys.isEmpty()) {
                return false;
            }
            return isDimSide0(rel.getInput(0), inputJoinKeys);
        }
        return false;
    }

    private static ImmutableIntList getInputIndices(
            List<RexNode> projects, ImmutableIntList joinKeys) {
        List<Integer> indices = new ArrayList<>();
        for (int k : joinKeys) {
            RexNode rexNode = projects.get(k);
            if (rexNode instanceof RexInputRef) {
                indices.add(((RexInputRef) rexNode).getIndex());
            } else {
                return ImmutableIntList.of();
            }
        }
        return ImmutableIntList.copyOf(indices);
    }

    private static ImmutableIntList extractPartitionKeysFromFactSide(
            RelNode rel, ImmutableIntList joinKeys) {
        ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel);
        if (partitionKeys.isEmpty()) {
            return ImmutableIntList.of();
        }
        List<Integer> keys = new ArrayList<>(joinKeys);
        keys.retainAll(partitionKeys);
        return ImmutableIntList.copyOf(keys);
    }

    private static boolean hasFilter(RelNode rel) {
        if (rel instanceof TableScan) {
            return false;
        }
        if (rel instanceof HepRelVertex) {
            return hasFilter(((HepRelVertex) rel).getCurrentRel());
        }
        if (rel instanceof Exchange || rel instanceof Project) {
            return hasFilter(rel.getInput(0));
        }
        if (rel instanceof Calc && ((Calc) rel).getProgram().getCondition() != null) {
            return true;
        }
        return rel instanceof Filter && ((Filter) rel).getCondition() != null;
    }

    private static ImmutableIntList inferPartitionKeysInFactSide(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return inferPartitionKeysInFactSide(((HepRelVertex) rel).getCurrentRel());
        } else if (rel instanceof Exchange || rel instanceof Filter) {
            return inferPartitionKeysInFactSide(rel.getInput(0));
        } else if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return ImmutableIntList.of();
            }
            if (!(table.tableSource() instanceof SupportsDynamicFiltering)) {
                return ImmutableIntList.of();
            }
            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            List<String> partitionKeys = catalogTable.getPartitionKeys();
            return ImmutableIntList.of(
                    partitionKeys.stream()
                            .map(i -> scan.getRowType().getFieldNames().indexOf(i))
                            .mapToInt(i -> i)
                            .toArray());
        } else if (rel instanceof Project) {
            ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel.getInput(0));
            if (partitionKeys.isEmpty()) {
                return partitionKeys;
            }
            List<RexNode> projects = ((Project) rel).getProjects();
            return getPartitionKeysAfterProject(projects, partitionKeys);
        } else if (rel instanceof Calc) {
            ImmutableIntList partitionKeys = inferPartitionKeysInFactSide(rel.getInput(0));
            if (partitionKeys.isEmpty()) {
                return partitionKeys;
            }
            Calc calc = (Calc) rel;
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(p -> calc.getProgram().expandLocalRef(p))
                            .collect(Collectors.toList());
            return getPartitionKeysAfterProject(projects, partitionKeys);
        }
        return ImmutableIntList.of();
    }

    private static ImmutableIntList getPartitionKeysAfterProject(
            List<RexNode> projects, ImmutableIntList partitionKeys) {
        List<Integer> newPartitionKeys = new ArrayList<>();
        for (int i = 0; i < projects.size(); ++i) {
            RexNode rexNode = projects.get(i);
            if (rexNode instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode).getIndex();
                if (partitionKeys.contains(index)) {
                    newPartitionKeys.add(i);
                }
            }
        }
        return ImmutableIntList.copyOf(newPartitionKeys);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
        return supportDynamicFilter(join);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
    }
}
