/*
 * Copyright © 2022 DATAMART LLC
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
package ru.datamart.prostore.query.execution.plugin.adqm.enrichment.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.plugin.adqm.base.factory.AdqmHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.model.schema.AdqmDtmTable;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.dto.AdqmExtendContext;
import ru.datamart.prostore.query.execution.plugin.adqm.enrichment.dto.DeltaToAdd;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.QueryGeneratorContext;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.SYSTEM_FIELDS;
import static ru.datamart.prostore.query.execution.plugin.adqm.enrichment.utils.SqlEnrichmentConditionUtil.*;


@Slf4j
@Service("adqmDmlQueryExtendService")
public class AdqmDmlQueryExtendService implements QueryExtendService {
    private static final int SCHEMA_INDEX = 0;
    private static final int TABLE_NAME_INDEX = 1;
    private static final int BY_ONE_TABLE = 0;
    private static final int ONE_TABLE = 1;
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmDmlQueryExtendService(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public RelNode extendQuery(QueryGeneratorContext ctx) {
        val extendContext = new AdqmExtendContext();
        val relBuilder = ctx.getRelBuilder();
        relBuilder.clear();
        return insertUnion(ctx, extendContext, relBuilder);
    }

    private RelNode insertUnion(QueryGeneratorContext ctx,
                                AdqmExtendContext extendContext,
                                RelBuilder relBuilder) {
        var iterateResult = iterateTree(ctx, extendContext, ctx.getRelNode().rel, ctx.isLocal(), 0);
        var result = iterateResult;
        Aggregate aggregate = null;
        if (iterateResult instanceof Aggregate) {
            aggregate = (Aggregate) iterateResult;
            iterateResult = aggregate.getInput();
            result = iterateResult;
        }

        if (!extendContext.getTableScans().isEmpty()) {
            val topSignConditions = extendContext.getTableScans().stream()
                    .map(tableScan -> createSignSubQuery(tableScan, true))
                    .collect(Collectors.toList());

            val topNode = relBuilder
                    .push(iterateResult)
                    .filter(topSignConditions.size() == ONE_TABLE ?
                            topSignConditions.get(BY_ONE_TABLE) :
                            relBuilder.call(getSignOperatorCondition(true), topSignConditions))
                    .build();

            val bottomSignConditions = extendContext.getTableScans().stream()
                    .map(tableScan -> createSignSubQuery(tableScan, false))
                    .collect(Collectors.toList());

            val bottomNode = relBuilder
                    .push(iterateResult)
                    .filter(bottomSignConditions.size() == ONE_TABLE ?
                            bottomSignConditions.get(BY_ONE_TABLE) :
                            relBuilder.call(getSignOperatorCondition(false), bottomSignConditions))
                    .build();

            result = relBuilder
                    .push(topNode)
                    .push(bottomNode)
                    .union(true)
                    .build();
        }

        if (aggregate != null) {
            relBuilder.push(aggregate.copy(aggregate.getTraitSet(), result, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()));
            return relBuilder.build();
        }

        return result;
    }

    private RelNode iterateTree(QueryGeneratorContext context,
                                AdqmExtendContext extendContext,
                                RelNode node,
                                boolean isLocal,
                                int depth) {
        val deltaIterator = context.getDeltaIterator();
        val relBuilder = context.getRelBuilder();
        val newInput = new ArrayList<RelNode>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                if (!context.getDeltaIterator().hasNext()) {
                    throw new DataSourceException("No parameters defined to enrich the request");
                }
                relBuilder.push(insertModifiedTableScan(context, extendContext, (TableScan) node, deltaIterator.next(), isLocal, depth));
            } else {
                relBuilder.push(node);
            }
            return relBuilder.build();
        }

        if (node instanceof Filter) {
            val filter = (Filter) node;
            val condition = iterateRexNode(context, extendContext, filter.getCondition(), depth);
            processInputs(context, extendContext, node, newInput, isLocal, depth);

            if (newInput.get(0) instanceof Filter) {
                Filter previousFilter = (Filter) newInput.get(0);
                RexNode previousCondition = previousFilter.getCondition();
                relBuilder.push(previousFilter.copy(previousFilter.getTraitSet(), previousFilter.getInput(),
                        relBuilder.call(SqlStdOperatorTable.AND, RexUtil.flattenAnd(Arrays.asList(condition, previousCondition)))));
            } else {
                relBuilder.push(filter.copy(node.getTraitSet(), newInput.get(0), condition));
            }

            return relBuilder.build();
        }

        processInputs(context, extendContext, node, newInput, isLocal, depth);

        if (node instanceof Project) {
            val project = (Project) node;
            val projects = project.getProjects();
            relBuilder.pushAll(newInput);
            addDeltaFiltersIfPresent(extendContext, relBuilder, depth);

            return relBuilder
                    .project(projects)
                    .build();
        }

        if (node instanceof Join) {
            if (extendContext.getDeltasToAdd().size() >= 2) {
                val relNode = newInput.remove(1);
                val deltaToAdd = extendContext.getDeltasToAdd().remove(1);
                relBuilder.push(relNode);
                val conditions = createDeltaConditions(relBuilder, deltaToAdd.getDeltaInformation());
                addCondition(relBuilder, Collections.singletonList(new TargetDeltaCondition(deltaToAdd.getTarget(), conditions)));
                newInput.add(relBuilder.build());
            }

            val join = (Join) node;
            val actualizedCondition = join.getCondition().accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    val index = inputRef.getIndex();
                    val initialLeftInputSize = node.getInput(0).getRowType().getFieldCount();
                    val newLeftInputSize = newInput.get(0).getRowType().getFieldCount();
                    if (index < initialLeftInputSize || initialLeftInputSize == newLeftInputSize) {
                        return inputRef;
                    }
                    val diff = newLeftInputSize - initialLeftInputSize;
                    return new RexInputRef(inputRef.getIndex() + diff, inputRef.getType());
                }
            });
            relBuilder.pushAll(newInput).join(join.getJoinType(), actualizedCondition);
            addDeltaFiltersIfPresent(extendContext, relBuilder, depth);
            return relBuilder.build();
        }

        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        addDeltaFiltersIfPresent(extendContext, relBuilder, depth);
        return relBuilder.build();
    }

    private void addDeltaFiltersIfPresent(AdqmExtendContext extendContext,
                                          RelBuilder relBuilder,
                                          int depth) {
        if (!extendContext.getDeltasToAdd().isEmpty()) {
            val targetConditions = new ArrayList<TargetDeltaCondition>();
            extendContext.getDeltasToAdd().removeIf(deltaToAdd -> {
                if (deltaToAdd.getDepth() > depth) {
                    val deltaConditions = createDeltaConditions(relBuilder, deltaToAdd.getDeltaInformation());
                    val targetTable = deltaToAdd.getTarget();
                    targetConditions.add(new TargetDeltaCondition(targetTable, deltaConditions));
                    return true;
                }
                return false;
            });

            if (!targetConditions.isEmpty()) {
                addCondition(relBuilder, targetConditions);
            }
        }
    }

    private void addCondition(RelBuilder relBuilder,
                              List<TargetDeltaCondition> targetDeltaConditions) {
        val allConditions = targetDeltaConditions.stream()
                .flatMap(targetDeltaCondition -> targetDeltaCondition.getDeltaConditions().stream())
                .collect(Collectors.toList());

        while (relBuilder.peek() instanceof Filter) {
            val filter = (Filter) relBuilder.build();
            val condition = filter.getCondition();
            allConditions.add(condition);
            relBuilder.push(filter.getInput());
        }

        val condition = relBuilder.call(SqlStdOperatorTable.AND, RexUtil.flattenAnd(allConditions));
        val relNode = relBuilder.build();
        val rexBuilder = relBuilder.getRexBuilder();
        val rexNodes = rexNodesWithoutSystemFields(relNode, rexBuilder, targetDeltaConditions);
        val logicalFilter = LogicalFilter.create(relNode, condition);
        relBuilder.push(logicalFilter).project(rexNodes);
    }

    private List<RexNode> rexNodesWithoutSystemFields(RelNode relNode, RexBuilder rexBuilder, List<TargetDeltaCondition> targets) {
        if (relNode instanceof Join) {
            val indexesToFilter = getNodeIndexesToFilter(relNode, targets);
            return indexesToFilter.stream()
                    .map(i -> rexBuilder.makeInputRef(relNode, i))
                    .collect(Collectors.toList());
        }
        return relNode.getRowType().getFieldList().stream()
                .filter(relDataTypeField -> SYSTEM_FIELDS.stream().noneMatch(relDataTypeField.getName()::startsWith))
                .map(relDataTypeField -> rexBuilder.makeInputRef(relNode, relDataTypeField.getIndex()))
                .collect(Collectors.toList());
    }

    private List<Integer> getNodeIndexesToFilter(RelNode relNode, List<TargetDeltaCondition> targets) {
        val node = (Join) relNode;
        val leftNode = node.getInput(0);
        val rightNode = node.getInput(1);
        val rowType = node.getRowType();

        boolean leftContainsNode = isContainNode(leftNode, targets);
        boolean rightContainsNode = isContainNode(rightNode, targets);

        val fieldsCount = rowType.getFieldCount();
        val leftFieldsCount = leftNode.getRowType().getFieldCount();

        val indexesToFilter = new ArrayList<Integer>();
        for (int i = 0; i < fieldsCount; i++) {
            if (leftContainsNode && i < leftFieldsCount && isSystemField(rowType, i)) {
                continue;
            }
            if (rightContainsNode && i >= leftFieldsCount && isSystemField(rowType, i)) {
                continue;
            }
            indexesToFilter.add(i);
        }
        return indexesToFilter;
    }

    private boolean isSystemField(RelDataType rowType, int i) {
        val field = rowType.getFieldList().get(i);
        return SYSTEM_FIELDS.stream().anyMatch(field.getName()::startsWith);
    }

    private boolean isContainNode(RelNode node, List<TargetDeltaCondition> targets) {
        if (node == null) {
            return false;
        }
        for (TargetDeltaCondition target : targets) {
            if (node == target.getTarget()) {
                return true;
            }
        }
        if (!node.getInputs().isEmpty()) {
            for (RelNode input : node.getInputs()) {
                if (isContainNode(input, targets)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void processInputs(QueryGeneratorContext context,
                               AdqmExtendContext extendContext,
                               RelNode node,
                               List<RelNode> newInput,
                               boolean isLocal,
                               int depth) {
        for (int i = 0; i < node.getInputs().size(); i++) {
            val input = node.getInputs().get(i);
            val isInputLocal = isLocal || isShard(node, i);
            newInput.add(iterateTree(context, extendContext, input, isInputLocal, depth + 1));
        }
    }

    private RexNode iterateRexNode(QueryGeneratorContext context,
                                   AdqmExtendContext extendContext,
                                   RexNode condition,
                                   int depth) {
        if (condition instanceof RexSubQuery) {
            val rexSubQuery = (RexSubQuery) condition;
            val relNode = iterateTree(context, extendContext, rexSubQuery.rel, true, depth + 1);
            return rexSubQuery.clone(relNode);
        }

        if (condition instanceof RexCall) {
            val rexCall = (RexCall) condition;

            val newOperands = new ArrayList<RexNode>();
            for (RexNode operand : rexCall.getOperands()) {
                newOperands.add(iterateRexNode(context, extendContext, operand, depth + 1));
            }

            return rexCall.clone(rexCall.type, newOperands);
        }

        return condition;
    }

    private RelNode insertModifiedTableScan(QueryGeneratorContext ctx,
                                            AdqmExtendContext extendContext,
                                            TableScan tableScan,
                                            DeltaInformation deltaInfo,
                                            boolean isLocal,
                                            int depth) {
        val relBuilder = RelBuilder
                .proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), tableScan.getTable().getRelOptSchema());
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val entity = tableScan.getTable().unwrap(AdqmDtmTable.class).getEntity();
        switch (entity.getEntityType()) {
            case WRITEABLE_EXTERNAL_TABLE:
                throw new DtmException("Enriched query should not contain WRITABLE EXTERNAL tables");
            case READABLE_EXTERNAL_TABLE:
                if (deltaInfo.getType() != DeltaType.WITHOUT_SNAPSHOT) {
                    throw new DtmException("FOR SYSTEM_TIME clause is not supported for external tables");
                }

                val names = entity.getExternalTableLocationPath().split("\\.");
                if (isLocal) {
                    names[1] = names[1] + "_shard";
                }

                return relBuilder.scan(names).build();
            default:
                return renameTableScan(ctx.getEnvName(), extendContext, deltaInfo, relBuilder, qualifiedName, isLocal, depth);
        }
    }

    private RelNode renameTableScan(String env,
                                    AdqmExtendContext extendContext,
                                    DeltaInformation deltaInfo,
                                    RelBuilder relBuilder,
                                    List<String> qualifiedName,
                                    boolean isLocal,
                                    int depth) {
        val tableNames = helperTableNamesFactory.create(env,
                qualifiedName.get(SCHEMA_INDEX),
                qualifiedName.get(TABLE_NAME_INDEX));
        val tableName = isLocal ? tableNames.toQualifiedActualShard() : tableNames.toQualifiedActual();
        val scan = (TableScan) relBuilder
                .scan(tableName).build();
        extendContext.getTableScans().add(scan);
        extendContext.getDeltasToAdd().add(new DeltaToAdd(deltaInfo, depth, scan));
        return scan;
    }

    private boolean isShard(RelNode parentNode, int inputIndex) {
        return parentNode instanceof Join && inputIndex > 0;
    }

    @Data
    private static class TargetDeltaCondition {
        private final RelNode target;
        private final List<RexNode> deltaConditions;
    }
}
