/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

@Slf4j
public class AdbDmlQueryExtendWithHistoryService implements QueryExtendService {

    @Override
    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        val relNode = iterateTree(context, context.getRelNode().rel);
        context.getRelBuilder().clear();
        return relNode;
    }

    private RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        val deltaIterator = context.getDeltaIterator();
        val relBuilder = context.getRelBuilder();
        val newInput = new ArrayList<RelNode>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                if (!context.getDeltaIterator().hasNext()) {
                    throw new DataSourceException("No parameters defined to enrich the request");
                }
                relBuilder.push(insertModifiedTableScan(relBuilder, node, deltaIterator.next()));
            } else {
                relBuilder.push(node);
            }
            return relBuilder.build();
        }

        if (node instanceof LogicalFilter) {
            val logicalFilter = (LogicalFilter) node;
            val condition = iterateRexNode(context, logicalFilter.getCondition());
            node.getInputs().forEach(input -> newInput.add(iterateTree(context, input)));
            relBuilder.push(logicalFilter.copy(node.getTraitSet(), newInput.get(0), condition));
            return relBuilder.build();
        }

        node.getInputs().forEach(input -> newInput.add(iterateTree(context, input)));
        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        return relBuilder.build();
    }

    private RexNode iterateRexNode(QueryGeneratorContext context, RexNode condition) {
        if (condition instanceof RexSubQuery) {
            val rexSubQuery = (RexSubQuery) condition;
            val relNode = iterateTree(context, rexSubQuery.rel);
            return rexSubQuery.clone(relNode);
        }

        if (condition instanceof RexCall) {
            val rexCall = (RexCall) condition;
            val newOperands = new ArrayList<RexNode>();
            for (RexNode operand : rexCall.getOperands()) {
                newOperands.add(iterateRexNode(context, operand));
            }

            return rexCall.clone(rexCall.type, newOperands);
        }

        return condition;
    }

    private RelNode insertModifiedTableScan(RelBuilder parentBuilder, RelNode tableScan, DeltaInformation deltaInfo) {
        val relBuilder = RelBuilder
                .proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), parentBuilder.getRelOptSchema());
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val mutableQualifiedName = new ArrayList<>(qualifiedName);

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        val rexNodes = new ArrayList<RexNode>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
        );
        val name = new StringBuilder(mutableQualifiedName.get(mutableQualifiedName.size() - 1));
        RelNode topRelNode;
        RelNode bottomRelNode;

        initHistoryTableName(mutableQualifiedName, name);
        switch (deltaInfo.getType()) {
            case STARTED_IN:
                topRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                initActualTableName(mutableQualifiedName, name);
                bottomRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                break;
            case FINISHED_IN:
                topRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                return relBuilder.push(topRelNode).build();
            case DATETIME:
            case WITHOUT_SNAPSHOT:
            case NUM:
                topRelNode = createTopRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                initActualTableName(mutableQualifiedName, name);
                bottomRelNode = createBottomRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, mutableQualifiedName);
                break;
            default:
                throw new DataSourceException(String.format("Incorrect delta type %s, expected values: %s!",
                        deltaInfo.getType(),
                        Arrays.toString(DeltaType.values())));
        }
        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    }

    private void initHistoryTableName(ArrayList<String> mutableQualifiedName, StringBuilder name) {
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + HISTORY_TABLE_SUFFIX);
    }

    private void initActualTableName(ArrayList<String> mutableQualifiedName, StringBuilder name) {
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + ACTUAL_TABLE_SUFFIX);
    }

    private RelNode createRelNodeDeltaStartedIn(DeltaInformation deltaInfo,
                                                RelBuilder relBuilder,
                                                List<RexNode> rexNodes,
                                                List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom())),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo()))
            )
        ).project(rexNodes).build();
    }

    private RelNode createRelNodeDeltaFinishedIn(DeltaInformation deltaInfo,
                                                 RelBuilder relBuilder,
                                                 List<RexNode> rexNodes,
                                                 List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom() - 1)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo() - 1)),
                relBuilder.call(SqlStdOperatorTable.EQUALS,
                    relBuilder.field(SYS_OP_ATTR),
                    relBuilder.literal(1))
            )
        ).project(rexNodes).build();
    }

    private RelNode createTopRelNodeDeltaNum(DeltaInformation deltaInfo,
                                             RelBuilder relBuilder,
                                             List<RexNode> rexNodes,
                                             List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnNum())),
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_ATTR),
                    relBuilder.literal(deltaInfo.getSelectOnNum()))
            )
        ).project(rexNodes).build();
    }

    private RelNode createBottomRelNodeDeltaNum(DeltaInformation deltaInfo,
                                                RelBuilder relBuilder,
                                                List<RexNode> rexNodes,
                                                List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).filter(
            relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                relBuilder.field(SYS_FROM_ATTR),
                relBuilder.literal(deltaInfo.getSelectOnNum()))).project(rexNodes).build();
    }

}
