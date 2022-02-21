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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;


@Slf4j
@Service("adgDmlQueryExtendService")
public class AdgDmlQueryExtendService implements QueryExtendService {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgDmlQueryExtendService(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        RelNode relNode = iterateTree(context, context.getRelNode().rel);
        context.getRelBuilder().clear();
        return relNode;
    }

    private RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        val newInput = new ArrayList<RelNode>();
        val relBuilder = context.getRelBuilder();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                if (!context.getDeltaIterator().hasNext()) {
                    throw new DataSourceException("No parameters defined to enrich the request");
                }
                relBuilder.push(insertModifiedTableScan(context, node, context.getDeltaIterator().next()));
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

    private RelNode insertModifiedTableScan(QueryGeneratorContext context, RelNode tableScan, DeltaInformation deltaInfo) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
            .create(tableScan.getCluster(),
                ((CalciteCatalogReader) context.getRelBuilder().getRelOptSchema())
                    .withSchemaPath(Collections.singletonList(deltaInfo.getSchemaName())));

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        List<RexNode> rexNodes = new ArrayList<>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
            rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
        );

        val qualifiedName = tableScan.getTable().getQualifiedName();
        val tableName = qualifiedName.get(qualifiedName.size() > 1 ? 1 : 0);
        val schemaName = deltaInfo.getSchemaName();
        val enrichQueryRequest = context.getEnrichQueryRequest();
        val tableNames = helperTableNamesFactory.create(enrichQueryRequest.getEnvName(),
            schemaName, tableName);
        RelNode topRelNode;
        RelNode bottomRelNode;

        switch (deltaInfo.getType()) {
            case STARTED_IN:
                topRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                bottomRelNode = createRelNodeDeltaStartedIn(deltaInfo, relBuilder, rexNodes, tableNames.getActual());
                break;
            case FINISHED_IN:
                topRelNode = createRelNodeDeltaFinishedIn(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                return relBuilder.push(topRelNode).build();
            case DATETIME:
            case WITHOUT_SNAPSHOT:
            case NUM:
                topRelNode = createTopRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, tableNames.getHistory());
                bottomRelNode = createBottomRelNodeDeltaNum(deltaInfo, relBuilder, rexNodes, tableNames.getActual());
                break;
            default:
                throw new DataSourceException(String.format("Incorrect delta type %s, expected values: %s!",
                    deltaInfo.getType(), DeltaType.values()));
        }

        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    }

    private RelNode createRelNodeDeltaStartedIn(DeltaInformation deltaInfo,
                                                RelBuilder relBuilder,
                                                List<RexNode> rexNodes,
                                                String tableName) {
        return relBuilder.scan(tableName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom())),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo()))
            )
        ).project(rexNodes).build();
    }

    private RelNode createRelNodeDeltaFinishedIn(DeltaInformation deltaInfo,
                                                 RelBuilder relBuilder,
                                                 List<RexNode> rexNodes,
                                                 String tableName) {
        return relBuilder.scan(tableName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnFrom() - 1)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnInterval().getSelectOnTo() - 1)),
                relBuilder.call(SqlStdOperatorTable.EQUALS,
                    relBuilder.field(SYS_OP_FIELD),
                    relBuilder.literal(1))
            )
        ).project(rexNodes).build();
    }

    private RelNode createTopRelNodeDeltaNum(DeltaInformation deltaInfo,
                                             RelBuilder relBuilder,
                                             List<RexNode> rexNodes,
                                             String tableName) {
        return relBuilder.scan(tableName).filter(
            relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    relBuilder.field(SYS_FROM_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnNum())),
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    relBuilder.field(SYS_TO_FIELD),
                    relBuilder.literal(deltaInfo.getSelectOnNum()))
            )
        ).project(rexNodes).build();
    }

    private RelNode createBottomRelNodeDeltaNum(DeltaInformation deltaInfo,
                                                RelBuilder relBuilder,
                                                List<RexNode> rexNodes,
                                                String tableName) {
        return relBuilder.scan(tableName).filter(
            relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                relBuilder.field(SYS_FROM_FIELD),
                relBuilder.literal(deltaInfo.getSelectOnNum()))).project(rexNodes).build();
    }

}
