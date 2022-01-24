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
package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaEntityType;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils.DROP_VIEW;

@Slf4j
@Component
public class InformationSchemaService {

    private static final int TABLE_NAME_COLUMN_INDEX = 0;
    private static final int ORDINAL_POSITION_COLUMN_INDEX = 1;
    private static final int COLUMN_NAME_COLUMN_INDEX = 2;
    private static final int DATA_TYPE_COLUMN_INDEX = 3;
    private static final int IS_NULLABLE_COLUMN_INDEX = 4;
    private static final String IS_NULLABLE_COLUMN_TRUE = "YES";
    private static final String MATERIALIZED_VIEW_PREFIX = "sys_";
    private static final String COMMENTS_DELIMETER = ";";

    private final ApplicationContext applicationContext;
    private final DdlQueryGenerator ddlQueryGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final HSQLClient client;
    private final InformationSchemaQueryFactory informationSchemaQueryFactory;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;

    @Autowired
    public InformationSchemaService(HSQLClient client,
                                    DatamartDao datamartDao,
                                    EntityDao entityDao,
                                    @Qualifier("hsqlDdlQueryGenerator") DdlQueryGenerator ddlQueryGenerator,
                                    ApplicationContext applicationContext,
                                    InformationSchemaQueryFactory informationSchemaQueryFactory,
                                    CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService) {
        this.applicationContext = applicationContext;
        this.ddlQueryGenerator = ddlQueryGenerator;
        this.datamartDao = datamartDao;
        this.entityDao = entityDao;
        this.client = client;
        this.informationSchemaQueryFactory = informationSchemaQueryFactory;
        this.materializedViewCacheService = materializedViewCacheService;
    }

    public Future<Void> update(Entity entity, String datamart, SqlKind sqlKind) {
        switch (sqlKind) {
            case CREATE_TABLE:
                return createTable(entity);
            case DROP_TABLE:
                return dropTable(entity);
            case CREATE_SCHEMA:
                return createSchema(datamart);
            case DROP_SCHEMA:
                return dropSchema(datamart);
            case CREATE_VIEW:
            case ALTER_VIEW:
                return createView(entity);
            case DROP_VIEW:
                return dropView(entity);
            case CREATE_MATERIALIZED_VIEW:
                return createMaterializedView(entity);
            case DROP_MATERIALIZED_VIEW:
                return dropMaterializedView(entity);
            default:
                throw new DtmException(String.format("Sql type not supported: %s", sqlKind));
        }
    }

    private void shutdown(Throwable throwable) {
        log.error("Shutdown application", throwable);
        val exitCode = SpringApplication.exit(applicationContext, () -> 1);
        System.exit(exitCode);
    }

    private Future<Void> dropTable(Entity entity) {
        if (CollectionUtils.isEmpty(entity.getDestination())) {
            return client.executeQuery(String.format(InformationSchemaUtils.DROP_TABLE, entity.getSchema(), entity.getName()))
                    .onFailure(this::shutdown);
        }
        return updateDestinationComment(entity);
    }

    private Future<Void> dropView(Entity entity) {
        return client.executeQuery(String.format(DROP_VIEW, entity.getSchema(), entity.getName()))
                .onFailure(this::shutdown);
    }

    private Future<Void> dropMaterializedView(Entity entity) {
        return Future.future(promise -> {
            if (CollectionUtils.isEmpty(entity.getDestination())) {
                Future<Void> dropTable = client.executeQuery(String.format(InformationSchemaUtils.DROP_TABLE, entity.getSchema(), entity.getName()));
                Future<Void> dropView = client.executeQuery(String.format(DROP_VIEW, entity.getSchema(), MATERIALIZED_VIEW_PREFIX + entity.getName()));
                CompositeFuture.join(dropView, dropTable)
                        .onSuccess(v -> promise.complete())
                        .onFailure(e -> {
                            shutdown(e);
                            promise.fail(e);
                        });
            } else {
                updateDestinationComment(entity)
                        .onSuccess(v -> promise.complete())
                        .onFailure(e -> {
                            shutdown(e);
                            promise.fail(e);
                        });
            }
        });
    }

    private Future<Void> createSchema(String datamart) {
        return client.executeQuery(String.format(InformationSchemaUtils.CREATE_SCHEMA, datamart))
                .onFailure(this::shutdown);
    }

    private Future<Void> dropSchema(String datamart) {
        return dropViews(datamart)
                .compose(ignore -> client.executeQuery(String.format(InformationSchemaUtils.DROP_SCHEMA, datamart)))
                .onFailure(this::shutdown);
    }

    private Future<Void> dropViews(String datamart) {
        return client.getQueryResult(String.format(InformationSchemaUtils.SELECT_VIEWS_BY_SCHEMA, datamart.toUpperCase()))
                .map(resultSet -> resultSet.getResults().stream()
                        .map(array -> String.format(DROP_VIEW, datamart, array.getString(0)))
                        .collect(Collectors.toList()))
                .compose(client::executeBatch);
    }

    private Future<Void> createTable(Entity entity) {
        return client.executeBatch(getEntitiesCreateQueries(Collections.singletonList(entity)))
                .onFailure(this::shutdown);
    }

    private Future<Void> createView(Entity entity) {
        return dropView(entity)
                .compose(v -> client.executeBatch(getEntitiesCreateQueries(Collections.singletonList(entity))))
                .onFailure(this::shutdown);
    }

    private Future<Void> createMaterializedView(Entity entity) {
        return client.executeBatch(getEntitiesCreateQueries(Collections.singletonList(entity)))
                .onFailure(this::shutdown);
    }

    private Future<Void> updateDestinationComment(Entity entity) {
        return client.executeQuery(commentOnTable(entity));
    }

    private String commentOnColumn(String schemaTable, String column, String comment) {
        return String.format(InformationSchemaUtils.COMMENT_ON_COLUMN, schemaTable, column, comment);
    }

    private String commentOnTable(Entity entity) {
        return String.format(InformationSchemaUtils.COMMENT_ON_TABLE, entity.getNameWithSchema(), getTableComment(entity));
    }

    private String createShardingKeyIndex(Entity entity) {
        val shardingKeyColumns = entity.getFields().stream()
                .filter(field -> field.getShardingOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        return String.format(InformationSchemaUtils.CREATE_SHARDING_KEY_INDEX,
                entity.getName(), entity.getNameWithSchema(), String.join(", ", shardingKeyColumns));
    }

    public Future<Void> initInformationSchema() {
        log.info("Information schema initialization started");
        return client.executeBatch(informationSchemaViewsQueries())
                .compose(v -> createSchemasFromDatasource())
                .compose(v -> initEntities())
                .onSuccess(v -> log.info("Information schema initialized successfully"));
    }

    private Future<Void> initEntities() {
        return Future.future(promise -> client.getQueryResult(informationSchemaQueryFactory.createInitEntitiesQuery())
                .onSuccess(resultSet -> {
                    try {
                        Map<String, List<EntityField>> fieldsByView = resultSet.getResults().stream()
                                .collect(Collectors.groupingBy(col -> col.getString(TABLE_NAME_COLUMN_INDEX),
                                        Collectors.mapping(this::createField, Collectors.toList())));

                        val entities = Arrays.stream(InformationSchemaView.values())
                                .flatMap(view -> {
                                    final String viewRealName = view.getRealName().toUpperCase();
                                    return Optional.ofNullable(fieldsByView.get(viewRealName))
                                            .map(fields -> createEntity(view, fields).stream())
                                            .orElseThrow(() -> new EntityNotExistsException(
                                                    InformationSchemaView.DTM_SCHEMA_NAME + "." + viewRealName));
                                })
                                .collect(Collectors.toList());

                        createLogicSchemaDatamartInDatasource()
                                .compose(r -> storeLogicSchemaInDatasource(entities))
                                .onSuccess(success -> promise.complete())
                                .onFailure(promise::fail);

                    } catch (Exception e) {
                        promise.fail(new DtmException("Error preparing entities for creating in information schema", e));
                    }
                })
                .onFailure(promise::fail));
    }

    private List<Entity> createEntity(final InformationSchemaView view, final List<EntityField> fields) {
        return Arrays.asList(
                Entity.builder()
                        .schema(InformationSchemaView.SCHEMA_NAME.toLowerCase())
                        .entityType(EntityType.VIEW)
                        .name(view.name().toLowerCase())
                        .viewQuery(String.format("select * from %s.%s", InformationSchemaView.SCHEMA_NAME.toLowerCase(),
                                view.getRealName()))
                        .fields(fields)
                        .build(),
                Entity.builder()
                        .schema(InformationSchemaView.SCHEMA_NAME.toLowerCase())
                        .entityType(EntityType.TABLE)
                        .name(view.getRealName().toLowerCase())
                        .fields(fields)
                        .build());
    }

    private EntityField createField(final JsonArray jsonArray) {
        return EntityField.builder()
                .ordinalPosition(jsonArray.getInteger(ORDINAL_POSITION_COLUMN_INDEX))
                .name(jsonArray.getString(COLUMN_NAME_COLUMN_INDEX).toLowerCase())
                .type(ColumnType.valueOf(jsonArray.getString(DATA_TYPE_COLUMN_INDEX)))
                .nullable(IS_NULLABLE_COLUMN_TRUE.equals(jsonArray.getString(IS_NULLABLE_COLUMN_INDEX)))
                .build();
    }

    private Future<Void> createLogicSchemaDatamartInDatasource() {
        return Future.future(promise ->
                createSchemaIfNotExists()
                        .onSuccess(r -> promise.complete())
                        .onFailure(error -> {
                            if (error instanceof DatamartAlreadyExistsException) {
                                promise.complete();
                            } else {
                                promise.fail(error);
                            }
                        }));
    }

    private Future<Void> createSchemaIfNotExists() {
        String schemaName = InformationSchemaView.SCHEMA_NAME.toLowerCase();
        return datamartDao.existsDatamart(schemaName)
                .compose(isExists -> isExists ? Future.succeededFuture() : datamartDao.createDatamart(schemaName));
    }

    private Future<Void> storeLogicSchemaInDatasource(List<Entity> entities) {
        return Future.future(p ->
                CompositeFuture.join(entities.stream()
                        .map(this::createEntityIfNotExists)
                        .collect(Collectors.toList()))
                        .onSuccess(success -> p.complete())
                        .onFailure(p::fail));
    }

    private Future<Void> createEntityIfNotExists(Entity entity) {
        return entityDao.existsEntity(entity.getSchema(), entity.getName())
                .compose(isExists -> isExists ? Future.succeededFuture() : entityDao.createEntity(entity));
    }

    private List<String> informationSchemaViewsQueries() {
        return Arrays.asList(
                String.format(InformationSchemaUtils.CREATE_SCHEMA, InformationSchemaView.DTM_SCHEMA_NAME),
                InformationSchemaUtils.LOGIC_SCHEMA_DATAMARTS,
                InformationSchemaUtils.LOGIC_SCHEMA_ENTITIES,
                InformationSchemaUtils.LOGIC_SCHEMA_COLUMNS,
                InformationSchemaUtils.LOGIC_SCHEMA_KEY_COLUMN_USAGE,
                InformationSchemaUtils.LOGIC_SCHEMA_ENTITY_CONSTRAINTS);
    }

    private Future<Void> createSchemasFromDatasource() {
        return datamartDao.getDatamarts()
                .map(datamarts -> datamarts.stream()
                        .filter(datamart -> !InformationSchemaView.SCHEMA_NAME.equalsIgnoreCase(datamart))
                        .collect(Collectors.toList()))
                .compose(this::createSchemas);
    }

    private Future<Void> createSchemas(List<String> datamarts) {
        val createSchemaQueries = datamarts.stream()
                .map(datamart -> String.format(InformationSchemaUtils.CREATE_SCHEMA, datamart))
                .collect(Collectors.toList());

        return client.executeBatch(createSchemaQueries)
                .compose(ignored -> getAllEntities(datamarts))
                .compose(entities -> client.executeBatch(getEntitiesCreateQueries(entities)));
    }

    private Future<List<Entity>> getAllEntities(List<String> datamarts) {
        return CompositeFuture.join(datamarts.stream()
                .map(this::getEntitiesByDatamart)
                .collect(Collectors.toList()))
                .map(entityResult -> entityResult.list().stream()
                        .flatMap(list -> ((List<Entity>) list).stream())
                        .collect(Collectors.toList()));
    }

    private Future<List<Entity>> getEntitiesByDatamart(String datamart) {
        return entityDao.getEntityNamesByDatamart(datamart)
                .compose(entityNames -> getEntitiesByNames(datamart, entityNames));
    }

    private Future<List<Entity>> getEntitiesByNames(String datamart, List<String> entitiesNames) {
        return Future.future(promise -> CompositeFuture.join(entitiesNames.stream()
                .map(entity -> entityDao.getEntity(datamart, entity))
                .collect(Collectors.toList()))
                .onSuccess(entityResult -> promise.complete(entityResult.list()))
                .onFailure(promise::fail));
    }

    private List<String> getEntitiesCreateQueries(List<Entity> entities) {
        List<String> viewEntities = new ArrayList<>();
        List<String> tableEntities = new ArrayList<>();
        List<String> commentQueries = new ArrayList<>();
        List<String> createShardingKeys = new ArrayList<>();
        entities.forEach(entity -> {
            if (EntityType.VIEW.equals(entity.getEntityType())) {
                viewEntities.add(ddlQueryGenerator.generateCreateViewQuery(entity, ""));
                commentQueries.addAll(getCommentQueries(entity));
            }
            if (EntityType.TABLE.equals(entity.getEntityType())) {
                tableEntities.add(ddlQueryGenerator.generateCreateTableQuery(entity));
                createShardingKeys.add(createShardingKeyIndex(entity));
                commentQueries.addAll(getCommentQueries(entity));
            }
            if (EntityType.MATERIALIZED_VIEW.equals(entity.getEntityType())) {
                viewEntities.add(ddlQueryGenerator.generateCreateViewQuery(entity, MATERIALIZED_VIEW_PREFIX));
                tableEntities.add(ddlQueryGenerator.generateCreateTableQuery(entity));
                createShardingKeys.add(createShardingKeyIndex(entity));
                commentQueries.addAll(getCommentQueries(entity));
                materializedViewCacheService.put(new EntityKey(entity.getSchema(), entity.getName()), new MaterializedViewCacheValue(entity));
            }
        });
        return Stream.of(tableEntities, viewEntities, commentQueries, createShardingKeys)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<String> getCommentQueries(Entity entity) {
        List<String> result = new ArrayList<>();
        entity.getFields()
                .forEach(field -> {
                    val type = field.getType();
                    if (needComment(type)) {
                        result.add(commentOnColumn(entity.getNameWithSchema(), field.getName(), type.toString()));
                    }
                });
        result.add(commentOnTable(entity));
        return result;
    }

    @NotNull
    private String getTableComment(Entity entity) {
        val comment = new StringBuilder();
        if (entity.getDestination() != null) {
            comment.append(entity.getDestination().stream()
                    .sorted()
                    .map(Enum::toString)
                    .collect(Collectors.joining(", ")));
        }
        comment.append(COMMENTS_DELIMETER);
        switch (entity.getEntityType()) {
            case TABLE:
                comment.append(InformationSchemaEntityType.BASE_TABLE.getValue());
                break;
            case VIEW:
                comment.append(InformationSchemaEntityType.VIEW.getValue());
                break;
            case MATERIALIZED_VIEW:
                comment.append(InformationSchemaEntityType.MATERIALIZED_VIEW.getValue());
                break;
            default:
                throw new DtmException(String.format("Unexpected entity type %s", entity.getEntityType()));
        }
        comment.append(COMMENTS_DELIMETER);
        return comment.toString();
    }

    private boolean needComment(ColumnType type) {
        switch (type) {
            case DOUBLE:
            case FLOAT:
            case INT32:
            case LINK:
            case INT:
            case VARCHAR:
            case CHAR:
            case UUID:
                return true;
            default:
                return false;
        }
    }
}
