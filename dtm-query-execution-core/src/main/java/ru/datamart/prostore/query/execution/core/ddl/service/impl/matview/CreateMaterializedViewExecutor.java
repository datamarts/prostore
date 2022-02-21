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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.matview;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateMaterializedView;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.visitors.SqlForbiddenNamesFinder;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.materializedview.MaterializedViewValidationException;
import ru.datamart.prostore.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.SetEntityState;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlType;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.core.ddl.utils.ValidationUtils.*;

@Slf4j
@Component
public class CreateMaterializedViewExecutor extends QueryResultDdlExecutor {
    private static final SqlPredicates VIEW_AND_TABLE_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final QueryParserService parserService;
    private final ColumnMetadataService columnMetadataService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DataSourcePluginService dataSourcePluginService;
    private final DtmRelToSqlConverter relToSqlConverter;
    private final InformationSchemaService informationSchemaService;

    @Autowired
    public CreateMaterializedViewExecutor(MetadataExecutor metadataExecutor,
                                          ServiceDbFacade serviceDbFacade,
                                          @Qualifier("coreLimitSqlDialect") SqlDialect sqlDialect,
                                          @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                          @Qualifier("materializedViewCacheService") CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                          LogicalSchemaProvider logicalSchemaProvider,
                                          ColumnMetadataService columnMetadataService,
                                          @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                                          MetadataCalciteGenerator metadataCalciteGenerator,
                                          DataSourcePluginService dataSourcePluginService,
                                          @Qualifier("coreRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter,
                                          InformationSchemaService informationSchemaService) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.entityCacheService = entityCacheService;
        this.materializedViewCacheService = materializedViewCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.parserService = parserService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.dataSourcePluginService = dataSourcePluginService;
        this.relToSqlConverter = relToSqlConverter;
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return updateContextAndValidate(context)
                .compose(unused -> parseSelect(((SqlCreateMaterializedView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .map(response -> {
                    checkTimestampFormat(response.getSqlNode());
                    return response;
                })
                .compose(response -> createMaterializedView(context, response));
    }

    protected Future<Void> updateContextAndValidate(DdlRequestContext context) {
        return Future.future(p -> {
            context.setDdlType(DdlType.CREATE_MATERIALIZED_VIEW);

            val datamartName = context.getDatamartName();
            val sqlNode = (SqlCreateMaterializedView) context.getSqlNode();

            val querySourceType = getQuerySourceType(sqlNode.getQuery());
            if (querySourceType == null || !dataSourcePluginService.hasSourceType(querySourceType)) {
                throw MaterializedViewValidationException.queryDataSourceInvalid(sqlNode.getName().toString());
            }

            context.setSourceType(querySourceType);

            val destination = sqlNode.getDestination().getDatasourceTypes();
            if (destination != null && !destination.isEmpty()) {
                val nonExistDestinations = destination.stream()
                        .filter(sourceType -> !dataSourcePluginService.hasSourceType(sourceType))
                        .collect(Collectors.toSet());
                if (!nonExistDestinations.isEmpty()) {
                    throw MaterializedViewValidationException.viewDataSourceInvalid(sqlNode.getName().toString(), nonExistDestinations);
                }
            }

            checkSystemColumnNames(sqlNode)
                    .compose(v -> checkSnapshotNotExist(sqlNode))
                    .compose(v -> checkEntitiesType(sqlNode, datamartName))
                    .onComplete(p);
        });
    }

    protected Future<QueryParserResponse> parseSelect(SqlNode viewQuery, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(viewQuery, datamart)
                .compose(datamarts -> parserService.parse(new QueryParserRequest(viewQuery, datamarts)));
    }

    protected Future<QueryResult> createMaterializedView(DdlRequestContext context, QueryParserResponse parserResponse) {
        return Future.future(p -> {
            val originalQuerySql = sqlNodeToString(context.getSqlNode());
            val selectSqlNode = getParsedSelect(parserResponse);
            replaceSqlSelectQuery(context, selectSqlNode);

            prepareEntityFuture(context, selectSqlNode, parserResponse.getSchema())
                    .compose(e -> validateQuery(e, parserResponse.getSchema()))
                    .compose(this::checkEntityNotExists)
                    .compose(v -> writeNewChangelogRecord(context.getDatamartName(), context.getEntity().getName(), originalQuerySql))
                    .compose(deltaOk -> executeRequest(context)
                            .compose(i -> entityDao.setEntityState(context.getEntity(), deltaOk, originalQuerySql, SetEntityState.CREATE)))
                    .onSuccess(v -> {
                        materializedViewCacheService.put(new EntityKey(context.getEntity().getSchema(), context.getEntity().getName()),
                                new MaterializedViewCacheValue(context.getEntity()));
                        p.complete(QueryResult.emptyResult());
                    })
                    .onFailure(p::fail);
        });
    }

    private Future<Entity> validateQuery(Entity entity, List<Datamart> datamarts) {
        if (datamarts.size() > 1) {
            throw MaterializedViewValidationException.multipleDatamarts(entity.getName(), datamarts, entity.getViewQuery());
        }

        if (datamarts.size() == 1) {
            val entityDatamart = entity.getSchema();
            val queryDatamart = datamarts.get(0).getMnemonic();
            if (!Objects.equals(entityDatamart, queryDatamart)) {
                throw MaterializedViewValidationException.differentDatamarts(entity.getName(), entityDatamart, queryDatamart);
            }
        }

        return informationSchemaService.validate(entity.getViewQuery())
                .map(ignored -> entity);
    }

    private Future<Entity> checkEntityNotExists(Entity entity) {
        return Future.future(p -> {
            val datamartName = entity.getSchema();
            val entityName = entity.getName();
            datamartDao.existsDatamart(datamartName)
                    .compose(existsDatamart -> existsDatamart ? entityDao.existsEntity(datamartName, entityName) : Future.failedFuture(new DatamartNotExistsException(datamartName)))
                    .onSuccess(existsEntity -> {
                        if (!existsEntity) {
                            p.complete(entity);
                        } else {
                            p.fail(new EntityAlreadyExistsException(entity.getNameWithSchema()));
                        }
                    })
                    .onFailure(p::fail);
        });
    }

    private SourceType getQuerySourceType(SqlNode sqlNode) {
        if (sqlNode instanceof SqlDataSourceTypeGetter) {
            return ((SqlDataSourceTypeGetter) sqlNode).getDatasourceType().getValue();
        }

        return null;
    }

    private SqlNode getParsedSelect(QueryParserResponse response) {
        return relToSqlConverter.convertWithoutStar(response.getRelNode().project());
    }

    @SneakyThrows
    protected void replaceSqlSelectQuery(DdlRequestContext context, SqlNode newSelectNode) {
        val matviewSql = (SqlCreateMaterializedView) context.getSqlNode();
        val selectList = ((SqlSelect) newSelectNode).getSelectList();
        val matviewName = matviewSql.getName().toString();
        if (matviewSql.getColumnList() == null) {
            throw MaterializedViewValidationException.columnsNotDefined(matviewName);
        }

        if (selectList != null) {
            val updatedSelectList = withColumnAliases(selectList, matviewSql.getColumnList(), matviewName);
            ((SqlSelect) newSelectNode).setSelectList(updatedSelectList);
        }

        val newSql = new SqlCreateMaterializedView(matviewSql.getParserPosition(), matviewSql.getName(), matviewSql.getColumnList(),
                matviewSql.getDistributedBy().getNodeList(), matviewSql.getDestination().getDatasourceTypesNode(),
                newSelectNode, matviewSql.isLogicalOnly());
        context.setSqlNode(newSql);
    }

    private SqlNodeList withColumnAliases(SqlNodeList selectList, SqlNodeList matViewColumns, String matView) {
        val updatedSelectList = new SqlNodeList(selectList.getParserPosition());

        val matViewColumnsCount = (int) matViewColumns.getList().stream()
                .filter(sqlNode -> sqlNode instanceof SqlColumnDeclaration)
                .count();
        val queryColumnsCount = selectList.size();
        if (queryColumnsCount != matViewColumnsCount) {
            throw MaterializedViewValidationException.columnCountConflict(matView, matViewColumnsCount, queryColumnsCount);
        }

        for (int i = 0; i < queryColumnsCount; i++) {
            val current = selectList.get(i);

            SqlBasicCall expression;
            if (current instanceof SqlBasicCall) {
                expression = (SqlBasicCall) current;
                List<SqlNode> operands = expression.getOperandList();

                if (expression.getOperator().getKind() == SqlKind.AS) {
                    SqlIdentifier newAlias = ((SqlIdentifier) operands.get(1)).setName(0, getMatViewColumnAlias(matViewColumns.get(i)));
                    expression.setOperand(1, newAlias);
                } else {
                    expression = buildAlias(expression, matViewColumns.get(i), expression.getParserPosition(), selectList.get(i).getParserPosition());
                }
            } else {
                expression = buildAlias(current, matViewColumns.get(i), current.getParserPosition(), selectList.get(i).getParserPosition());
            }
            updatedSelectList.add(expression);
        }

        return updatedSelectList;
    }

    private SqlBasicCall buildAlias(SqlNode queryColumn, SqlNode matViewColumn, SqlParserPos aliasPos, SqlParserPos pos) {
        return new SqlBasicCall(
                new SqlAsOperator(),
                new SqlNode[]{
                        queryColumn,
                        new SqlIdentifier(
                                getMatViewColumnAlias(matViewColumn),
                                aliasPos
                        )
                },
                pos
        );
    }

    private String getMatViewColumnAlias(SqlNode column) {
        return ((SqlIdentifier) ((SqlColumnDeclaration) column).getOperandList().get(0)).getSimple();
    }

    private Future<Void> checkSystemColumnNames(SqlNode sqlNode) {
        return Future.future(p -> {
            val sqlForbiddenNamesFinder = new SqlForbiddenNamesFinder();
            sqlNode.accept(sqlForbiddenNamesFinder);
            if (!sqlForbiddenNamesFinder.getFoundForbiddenNames().isEmpty()) {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql(),
                        String.format("View query contains forbidden system names: %s", sqlForbiddenNamesFinder.getFoundForbiddenNames())));
                return;
            }

            p.complete();
        });
    }

    private Future<Void> checkSnapshotNotExist(SqlNode sqlNode) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(sqlNode).findSnapshots();
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql()));
            }
        });
    }

    private Future<Void> checkEntitiesType(SqlNode sqlNode, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(sqlNode).findNodes(VIEW_AND_TABLE_PREDICATE, true);
            final List<Future> entityFutures = getEntitiesFutures(contextDatamartName, sqlNode, nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Entity> entities = result.list();
                        if (entities.stream().anyMatch(entity -> entity.getEntityType() != EntityType.TABLE)) {
                            promise.fail(new ViewDisalowedOrDirectiveException(
                                    sqlNode.toSqlString(sqlDialect).getSql()));
                        }
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getEntitiesFutures(String contextDatamartName, SqlNode sqlNode, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = contextDatamartName;
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }

            tableName = table.orElseThrow(() -> new DtmException(String.format("Can't extract table name from query %s",
                    sqlNode.toSqlString(sqlDialect).toString())));

            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    private Future<Entity> prepareEntityFuture(DdlRequestContext ctx, SqlNode viewQuery, List<Datamart> datamarts) {
        return columnMetadataService.getColumnMetadata(new QueryParserRequest(viewQuery, datamarts))
                .map(columnMetadata -> toMaterializedViewEntity(ctx, viewQuery, columnMetadata));
    }

    private Entity toMaterializedViewEntity(DdlRequestContext ctx, SqlNode viewQuery, List<ColumnMetadata> columnMetadata) {
        val sqlCreateMaterializedView = (SqlCreateMaterializedView) ctx.getSqlNode();

        val destination = Optional.ofNullable(sqlCreateMaterializedView.getDestination().getDatasourceTypes())
                .filter(sourceTypes -> !sourceTypes.isEmpty())
                .orElse(dataSourcePluginService.getSourceTypes());

        val viewQueryString = viewQuery.toSqlString(sqlDialect)
                .getSql()
                .replace("\n", " ").replace("\r", "");

        val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreateMaterializedView);
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);
        entity.setDestination(destination);
        entity.setViewQuery(viewQueryString);
        entity.setMaterializedDeltaNum(null);
        entity.setMaterializedDataSource(ctx.getSourceType());

        validateFields(entity, columnMetadata);
        setNullability(entity);

        ctx.setEntity(entity);
        ctx.setDatamartName(entity.getSchema());
        return entity;
    }

    private void setNullability(Entity entity) {
        for (EntityField field : entity.getFields()) {
            if (field.getPrimaryOrder() != null || field.getShardingOrder() != null) {
                field.setNullable(false);
            }
        }
    }

    private void validateFields(Entity entity, List<ColumnMetadata> columnMetadata) {
        checkEntityNames(entity);
        checkRequiredKeys(entity.getFields());
        checkCharFieldsSize(entity.getFields());
        checkFieldsMatch(entity, columnMetadata);
        checkFieldsDuplication(entity.getFields());
    }

    private void checkFieldsMatch(Entity entity, List<ColumnMetadata> queryColumns) {
        List<EntityField> viewFields = entity.getFields();
        if (viewFields.size() != queryColumns.size()) {
            throw MaterializedViewValidationException.columnCountConflict(entity.getName(), entity.getFields().size(), queryColumns.size());
        }
    }

    @Override
    public String getOperationKind() {
        return OperationNames.CREATE_MATERIALIZED_VIEW;
    }
}
