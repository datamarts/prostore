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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.view;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateView;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.SetEntityState;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.base.utils.InformationSchemaUtils;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.utils.SqlPreparer;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ru.datamart.prostore.query.execution.core.ddl.utils.ValidationUtils.checkEntityNames;
import static ru.datamart.prostore.query.execution.core.ddl.utils.ValidationUtils.checkTimestampFormat;

@Slf4j
@Component
public class CreateViewExecutor extends QueryResultDdlExecutor {
    private static final SqlPredicates VIEW_AND_TABLE_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();
    private static final SqlPredicates OTHER_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.OTHER))
            .build();
    private static final String COLLATE = "COLLATE";
    protected final EntityDao entityDao;
    protected final CacheService<EntityKey, Entity> entityCacheService;
    protected final LogicalSchemaProvider logicalSchemaProvider;
    protected final QueryParserService parserService;
    private final ColumnMetadataService columnMetadataService;
    private final DtmRelToSqlConverter relToSqlConverter;
    private final InformationSchemaService informationSchemaService;

    @Autowired
    public CreateViewExecutor(MetadataExecutor metadataExecutor,
                              ServiceDbFacade serviceDbFacade,
                              @Qualifier("coreLimitSqlDialect") SqlDialect sqlDialect,
                              @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                              LogicalSchemaProvider logicalSchemaProvider,
                              ColumnMetadataService columnMetadataService,
                              @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                              @Qualifier("coreRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter,
                              InformationSchemaService informationSchemaService) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.entityCacheService = entityCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.parserService = parserService;
        this.relToSqlConverter = relToSqlConverter;
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> parseSelect(((SqlCreateView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .map(parserResponse -> {
                    checkTimestampFormat(parserResponse.getSqlNode());
                    return parserResponse;
                })
                .compose(response -> getCreateViewContext(context, response))
                .compose(ctx -> writeViewChangelodRecord(ctx)
                        .compose(delta -> createOrReplaceEntity(ctx, delta)));
    }

    protected Future<Void> checkViewQuery(DdlRequestContext context) {
        val datamartName = context.getDatamartName();
        val sqlSelectTree = new SqlSelectTree(context.getSqlNode());
        return checkCollate(sqlSelectTree)
                .compose(v -> checkSnapshotNotExist(sqlSelectTree))
                .compose(v -> checkEntitiesType(sqlSelectTree, datamartName));
    }

    protected Future<QueryParserResponse> parseSelect(SqlNode viewQuery, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(viewQuery, datamart)
                .compose(datamarts -> parserService.parse(new QueryParserRequest(viewQuery, datamarts)));
    }

    protected Future<CreateViewContext> getCreateViewContext(DdlRequestContext context, QueryParserResponse parserResponse) {
        return Future.future(p -> {
            val selectSqlNode = getParsedSelect(parserResponse);
            val isCreateOrReplace = SqlPreparer.isCreateOrReplace(context.getRequest().getQueryRequest().getSql());
            replaceSqlSelectQuery(context, isCreateOrReplace, selectSqlNode);
            getEntityFuture(context, selectSqlNode, parserResponse.getSchema())
                    .compose(this::validateQuery)
                    .compose(entity -> checkEntity(entity, isCreateOrReplace))
                    .map(entity -> {
                        context.setEntity(entity);
                        return CreateViewContext.builder()
                                .createOrReplace(isCreateOrReplace)
                                .viewEntity(entity)
                                .changeQuery(sqlNodeToString(context.getSqlNode()))
                                .build();
                    })
                    .onComplete(p);
        });
    }

    private Future<Entity> validateQuery(Entity entity) {
        return informationSchemaService.validate(entity.getViewQuery())
                .map(ignored -> entity);
    }

    protected Future<Entity> checkEntity(Entity generatedEntity, boolean orReplace) {
        return Future.future(p -> {
            checkEntityNames(generatedEntity);

            entityDao.getEntity(generatedEntity.getSchema(), generatedEntity.getName())
                    .onSuccess(entity -> {
                        if (EntityType.VIEW != entity.getEntityType()) {
                            p.fail(new DtmException(String.format("Entity %s is not a view", entity.getName())));
                        } else if (orReplace) {
                            p.complete(generatedEntity);
                        } else {
                            p.fail(new EntityAlreadyExistsException(entity.getNameWithSchema()));
                        }
                    })
                    .onFailure(err -> {
                        if (err instanceof EntityNotExistsException) {
                            p.complete(generatedEntity);
                        } else {
                            p.fail(err);
                        }
                    });
        });
    }

    protected Future<OkDelta> writeViewChangelodRecord(CreateViewContext ctx) {
        return writeNewChangelogRecord(ctx.getViewEntity().getSchema(), ctx.getViewEntity().getName(), ctx.getChangeQuery());
    }

    private SqlNode getParsedSelect(QueryParserResponse response) {
        return relToSqlConverter.convertWithoutStar(response.getRelNode().project());
    }

    @SneakyThrows
    protected void replaceSqlSelectQuery(DdlRequestContext context, boolean replace, SqlNode newSelectNode) {
        val sql = (SqlCreateView) context.getSqlNode();
        val newSql = new SqlCreateView(sql.getParserPosition(), replace, sql.getName(), sql.getColumnList(), newSelectNode);
        context.setSqlNode(newSql);
    }

    private Future<QueryResult> createOrReplaceEntity(CreateViewContext ctx, OkDelta deltaOk) {
        val viewEntity = ctx.getViewEntity();
        entityCacheService.remove(new EntityKey(viewEntity.getSchema(), viewEntity.getName()));
        return Future.future(p -> entityDao.setEntityState(viewEntity, deltaOk, ctx.getChangeQuery(), SetEntityState.CREATE)
                .onSuccess(ar -> p.complete(QueryResult.emptyResult()))
                .onFailure(error -> {
                    if (checkCreateOrReplace(ctx.isCreateOrReplace(), error)) {
                        entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName())
                                .compose(this::checkEntityType)
                                .compose(r -> entityDao.setEntityState(viewEntity, deltaOk, ctx.getChangeQuery(), SetEntityState.UPDATE))
                                .map(v -> QueryResult.emptyResult())
                                .onSuccess(p::complete)
                                .onFailure(p::fail);
                    } else {
                        p.fail(error);
                    }
                })
        );
    }

    private Future<Void> checkCollate(SqlSelectTree selectTree) {
        val containsCollate = selectTree.findNodes(OTHER_PREDICATE, true).stream()
                .map(SqlTreeNode::getNode)
                .filter(SqlBasicCall.class::isInstance)
                .anyMatch(node -> {
                    val call = (SqlBasicCall) node;
                    return COLLATE.equals(call.getOperator().getName());
                });
        if (containsCollate) {
            return Future.failedFuture(new ViewDisalowedOrDirectiveException(selectTree.getRoot().getNode().toSqlString(sqlDialect).getSql(),
                    "Unsupported 'COLLATE' clause in view's query"));
        }
        return Future.succeededFuture();
    }

    private Future<Void> checkSnapshotNotExist(SqlSelectTree selectTree) {
        if (!selectTree.findSnapshots().isEmpty()) {
            return Future.failedFuture(new ViewDisalowedOrDirectiveException(selectTree.getRoot().getNode().toSqlString(sqlDialect).getSql()));
        }
        return Future.succeededFuture();
    }

    private Future<Void> checkEntitiesType(SqlSelectTree selectTree, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = selectTree.findNodes(VIEW_AND_TABLE_PREDICATE, true);
            final List<Future> entityFutures = getEntitiesFutures(contextDatamartName, selectTree.getRoot().getNode(), nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Entity> entities = result.list();
                        if (entities.stream().anyMatch(entity -> entity.getEntityType() != EntityType.TABLE)) {
                            promise.fail(new ViewDisalowedOrDirectiveException(
                                    selectTree.getRoot().getNode().toSqlString(sqlDialect).getSql()));
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
            if (table.isPresent()) {
                tableName = table.get();
            } else {
                throw new DtmException(String.format("Can't extract table name from query %s",
                        sqlNode.toSqlString(sqlDialect).toString()));
            }

            if (InformationSchemaUtils.INFORMATION_SCHEMA.equalsIgnoreCase(datamartName)) {
                throw new DtmException(String.format("Using of INFORMATION_SCHEMA is forbidden [%s.%s]", datamartName, tableName));
            }

            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    private Future<Entity> getEntityFuture(DdlRequestContext ctx, SqlNode viewQuery, List<Datamart> datamarts) {
        return columnMetadataService.getColumnMetadata(new QueryParserRequest(viewQuery, datamarts))
                .map(columnMetadata -> toViewEntity(ctx, viewQuery, columnMetadata));
    }

    @SneakyThrows
    private Entity toViewEntity(DdlRequestContext ctx, SqlNode viewQuery, List<ColumnMetadata> columnMetadata) {
        val tree = new SqlSelectTree(ctx.getSqlNode());
        val viewNameNode = SqlPreparer.getViewNameNode(tree);
        val schemaName = viewNameNode.tryGetSchemaName()
                .orElseThrow(() -> new DtmException("Unable to get schema of view"));
        val viewName = viewNameNode.tryGetTableName()
                .orElseThrow(() -> new DtmException("Unable to get name of view"));
        val viewQueryString = viewQuery.toSqlString(sqlDialect)
                .getSql()
                .replace("\n", " ").replace("\r", "");
        ctx.setDatamartName(schemaName);
        return Entity.builder()
                .name(viewName)
                .schema(schemaName)
                .entityType(EntityType.VIEW)
                .viewQuery(viewQueryString)
                .fields(getEntityFields(columnMetadata))
                .build();
    }

    private List<EntityField> getEntityFields(List<ColumnMetadata> columnMetadata) {
        return IntStream.range(0, columnMetadata.size())
                .mapToObj(position -> toEntityField(columnMetadata.get(position), position))
                .collect(Collectors.toList());
    }

    private EntityField toEntityField(ColumnMetadata cm, int position) {
        return EntityField.builder()
                .name(cm.getName())
                .nullable(true)
                .type(cm.getType())
                .size(cm.getSize())
                .ordinalPosition(position)
                .build();
    }

    @SneakyThrows
    private boolean checkCreateOrReplace(boolean isCreateOrReplace, Throwable error) {
        // if there is an exception <entity already exists> and <orReplace> is true
        // then continue
        return error instanceof EntityAlreadyExistsException && isCreateOrReplace;
    }

    protected Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        }
        return Future.failedFuture(new EntityNotExistsException(entity.getName()));
    }

    @Override
    public String getOperationKind() {
        return OperationNames.CREATE_VIEW;
    }

    @Data
    @Builder
    protected static final class CreateViewContext {
        private final boolean createOrReplace;
        private final Entity viewEntity;
        private final String changeQuery;
    }
}
