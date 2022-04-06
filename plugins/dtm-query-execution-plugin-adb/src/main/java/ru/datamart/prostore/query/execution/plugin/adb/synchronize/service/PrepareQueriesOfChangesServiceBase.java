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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.*;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.delta.SelectOnInterval;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.lang.String.format;

public abstract class PrepareQueriesOfChangesServiceBase implements PrepareQueriesOfChangesService {
    private static final SqlPredicates COLUMN_SELECT = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqFromStart(SqlKind.SELECT))
            .anyOf(SqlPredicatePart.eq(SqlKind.OTHER))
            .build();
    private static final int SYS_OP_MODIFIED = 0;
    private static final int SYS_OP_DELETED = 1;

    private final QueryParserService parserService;
    private final ColumnsCastService columnsCastService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final boolean skipNotNullableFields;

    protected PrepareQueriesOfChangesServiceBase(QueryParserService parserService,
                                                 ColumnsCastService columnsCastService,
                                                 QueryEnrichmentService adbQueryEnrichmentService,
                                                 boolean skipNotNullableFields) {
        this.parserService = parserService;
        this.columnsCastService = columnsCastService;
        this.queryEnrichmentService = adbQueryEnrichmentService;
        this.skipNotNullableFields = skipNotNullableFields;
    }

    @Override
    public Future<PrepareRequestOfChangesResult> prepare(PrepareRequestOfChangesRequest request) {
        return parserService.parse(new QueryParserRequest(request.getViewQuery(), request.getDatamarts(), request.getEnvName()))
                .compose(parserResponse -> columnsCastService.apply(parserResponse, EntityFieldUtils.getFieldTypes(request.getEntity())))
                .compose(sqlNode -> prepareQueriesOfChanges((SqlSelect) sqlNode, request));
    }

    private Future<PrepareRequestOfChangesResult> prepareQueriesOfChanges(SqlSelect sqlNode, PrepareRequestOfChangesRequest request) {
        return Future.future(promise -> {
            val sqlNodeTree = new SqlSelectTree(sqlNode);
            val allTableAndSnapshots = sqlNodeTree.findAllTableAndSnapshots();

            if (allTableAndSnapshots.isEmpty()) {
                throw new DtmException("No tables in query");
            }

            val cnFrom = request.getDeltaToBe().getCnFrom();
            val cnTo = request.getDeltaToBe().getCnTo();

            val futures = new ArrayList<Future>(2);
            if (allTableAndSnapshots.size() > 1 || isAggregatedQuery(sqlNode)) {
                futures.add(prepareMultipleRecordsQuery(sqlNode, request, cnTo, request.getBeforeDeltaCnTo(), SYS_OP_MODIFIED));
                futures.add(prepareMultipleRecordsQuery(sqlNode, request, request.getBeforeDeltaCnTo(), cnTo, SYS_OP_DELETED));
            } else {
                futures.add(enrichQueryWithDelta(sqlNode, request, cnFrom, cnTo, DeltaType.STARTED_IN, SYS_OP_MODIFIED));
                futures.add(enrichQueryWithDelta(sqlNode, request, cnFrom, cnTo, DeltaType.FINISHED_IN, SYS_OP_DELETED));
            }

            CompositeFuture.join(futures)
                    .onSuccess(event -> {
                        val result = event.<String>list();
                        promise.complete(new PrepareRequestOfChangesResult(result.get(0), result.get(1)));
                    })
                    .onFailure(promise::fail);
        });
    }

    private boolean isAggregatedQuery(SqlSelect sqlSelect) {
        return sqlSelect.getGroup() != null || SqlNodeUtil.containsAggregates(sqlSelect);
    }

    private Future<String> prepareMultipleRecordsQuery(SqlNode sqlNode, PrepareRequestOfChangesRequest request,
                                                       long cnCurrent, long cnPrevious, int sysOp) {
        return Future.future(promise -> {
            val currentStateQuery = enrichQueryWithDelta(sqlNode, request, cnCurrent, cnCurrent, DeltaType.NUM, sysOp);
            val previousStateQuery = enrichQueryWithDelta(sqlNode, request, cnPrevious, cnPrevious, DeltaType.NUM, sysOp);
            CompositeFuture.join(Arrays.asList(currentStateQuery, previousStateQuery))
                    .onSuccess(queries -> {
                        val result = queries.<String>list();
                        promise.complete(result.get(0) + " EXCEPT " + result.get(1));
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> enrichQueryWithDelta(SqlNode originalSqlNode,
                                                PrepareRequestOfChangesRequest request,
                                                long cnFrom,
                                                long cnTo,
                                                DeltaType deltaType,
                                                int sysOp) {
        return Future.future(promise -> {
            val sqlNode = SqlNodeUtil.copy(originalSqlNode);
            val sqlNodesTree = new SqlSelectTree(sqlNode);
            val deltaInformations = new ArrayList<DeltaInformation>();
            sqlNodesTree.findAllTableAndSnapshots().forEach(sqlTreeNode -> {
                val deltaInformation = addDeltaToTableQuery(sqlNodesTree, sqlTreeNode, deltaType, cnFrom, cnTo, request.getDatamarts());
                deltaInformations.add(deltaInformation);
            });

            if (sysOp == SYS_OP_DELETED) {
                removeNonPkNullableColumns(request.getEntity(), sqlNodesTree);
            }

            parserService.parse(new QueryParserRequest(sqlNode, request.getDatamarts(), request.getEnvName()))
                    .compose(parserResponse -> {
                        val enrichQueryRequest = EnrichQueryRequest.builder()
                                .envName(request.getEnvName())
                                .deltaInformations(deltaInformations)
                                .relNode(parserResponse.getRelNode())
                                .calciteContext(parserResponse.getCalciteContext())
                                .build();
                        return queryEnrichmentService.enrich(enrichQueryRequest);
                    })
                    .onComplete(promise);
        });
    }

    private void removeNonPkNullableColumns(Entity entity, SqlSelectTree sqlNodesTree) {
        val columnsNode = sqlNodesTree.findNodes(COLUMN_SELECT, true);
        if (columnsNode.size() != 1) {
            throw new DtmException(format("Expected one node contain columns, got: %s", columnsNode.size()));
        }
        val sqlTreeNode = columnsNode.get(0);
        val columnNodeList = (SqlNodeList) sqlTreeNode.getNode();
        val filteredSelectNodes = new ArrayList<SqlNode>(columnNodeList.size());
        entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .forEach(field -> {
                    if (field.getPrimaryOrder() != null || (!field.getNullable() && !skipNotNullableFields)) {
                        filteredSelectNodes.add(columnNodeList.get(field.getOrdinalPosition()));
                    }
                });
        columnNodeList.getList().clear();
        columnNodeList.getList().addAll(filteredSelectNodes);
    }

    private DeltaInformation addDeltaToTableQuery(SqlSelectTree sqlNodesTree, SqlTreeNode sqlTreeNode, DeltaType deltaType,
                                                  long cnFrom, long cnTo, List<Datamart> datamarts) {
        var tableTreeNode = sqlTreeNode;
        var alias = "";
        if (sqlTreeNode.getNode().getKind() == SqlKind.AS) {
            val asNodes = sqlNodesTree.findNodesByParent(sqlTreeNode);
            tableTreeNode = asNodes.get(0);
            alias = ((SqlIdentifier) asNodes.get(1).getNode()).names.get(0);
        }

        val tableSqlNode = (SqlIdentifier) tableTreeNode.getNode();
        val parserPos = tableSqlNode.getParserPosition();
        val schemaName = tableSqlNode.names.get(0);
        val tableName = tableSqlNode.names.get(1);

        SelectOnInterval builderInterval = null;
        Long builderDeltaNum = null;
        var deltaTypeToSet = deltaType;
        if (isReadableExternalTable(schemaName, tableName, datamarts)) {
            builderDeltaNum = 0L;
            deltaTypeToSet = DeltaType.WITHOUT_SNAPSHOT;
        } else if (deltaType == DeltaType.FINISHED_IN || deltaType == DeltaType.STARTED_IN) {
            builderInterval = new SelectOnInterval(cnFrom, cnTo);
        } else {
            builderDeltaNum = cnTo;
        }

        return DeltaInformation.builder()
                .pos(parserPos)
                .schemaName(schemaName)
                .tableName(tableName)
                .tableAlias(alias)
                .type(deltaTypeToSet)
                .selectOnInterval(builderInterval)
                .selectOnNum(builderDeltaNum)
                .isLatestUncommittedDelta(false)
                .build();
    }

    private boolean isReadableExternalTable(String schemaName, String tableName, List<Datamart> datamarts) {
        return datamarts.stream()
                .flatMap(datamart -> datamart.getEntities().stream())
                .anyMatch(entity -> entity.getName().equalsIgnoreCase(tableName) &&
                        entity.getSchema().equalsIgnoreCase(schemaName) &&
                        entity.getEntityType() == EntityType.READABLE_EXTERNAL_TABLE);
    }

}
