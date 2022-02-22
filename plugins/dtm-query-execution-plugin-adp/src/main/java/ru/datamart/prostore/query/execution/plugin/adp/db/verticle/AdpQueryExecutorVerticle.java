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
package ru.datamart.prostore.query.execution.plugin.adp.db.verticle;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpProperties;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service("adpQueryExecutor")
public class AdpQueryExecutorVerticle extends AbstractVerticle implements DatabaseExecutor {
    private static final DeliveryOptions DEFAULT_DELIVERY_OPTIONS = new DeliveryOptions()
            .setSendTimeout(86400000L);

    private final String database;
    private final AdpProperties adpProperties;
    private final SqlTypeConverter fromAdpSqlTypeConverter;
    private final SqlTypeConverter toAdpSqlTypeConverter;

    private final Map<String, AdpExecutorTask> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();

    public AdpQueryExecutorVerticle(@Value("${core.env.name}") String database,
                                    AdpProperties adpProperties,
                                    @Qualifier("fromAdpSqlTypeConverter") SqlTypeConverter fromAdpSqlTypeConverter,
                                    @Qualifier("toAdpSqlTypeConverter") SqlTypeConverter toAdpSqlTypeConverter) {
        this.database = database;
        this.adpProperties = adpProperties;
        this.fromAdpSqlTypeConverter = fromAdpSqlTypeConverter;
        this.toAdpSqlTypeConverter = toAdpSqlTypeConverter;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(adpProperties.getExecutorsCount());
        vertx.deployVerticle(() -> new AdpQueryExecutorTaskVerticle(database, adpProperties, fromAdpSqlTypeConverter, toAdpSqlTypeConverter, taskMap, resultMap),
                deploymentOptions, ar -> {
                    if (ar.succeeded()) {
                        startPromise.complete();
                    } else {
                        startPromise.fail(ar.cause());
                    }
                });
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE_WITH_CURSOR, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters params, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .params(params)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdpExecutorTopic.EXECUTE_WITH_PARAMS, request);
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .sql(sql)
                    .build();
            sendRequestWithoutResult(promise, AdpExecutorTopic.EXECUTE_UPDATE, request);
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(promise -> {
            AdpExecutorTask request = AdpExecutorTask.builder()
                    .preparedStatementRequests(requests)
                    .build();
            sendRequestWithoutResult(promise, AdpExecutorTopic.EXECUTE_IN_TRANSACTION, request);
        });
    }

    private void sendRequestWithResult(Promise<List<Map<String, Object>>> promise, AdpExecutorTopic topic, AdpExecutorTask request) {
        String key = UUID.randomUUID().toString();
        taskMap.put(key, request);
        vertx.eventBus().request(topic.getTopic(), key, DEFAULT_DELIVERY_OPTIONS, ar -> {
            taskMap.remove(key);
            if (ar.succeeded()) {
                promise.handle((AsyncResult<List<Map<String, Object>>>) resultMap.remove(key));
            } else {
                promise.fail(ar.cause());
            }
        });
    }

    private void sendRequestWithoutResult(Promise<Void> promise, AdpExecutorTopic topic, AdpExecutorTask request) {
        String key = UUID.randomUUID().toString();
        taskMap.put(key, request);
        vertx.eventBus().request(topic.getTopic(), key, DEFAULT_DELIVERY_OPTIONS, ar -> {
            taskMap.remove(key);
            if (ar.succeeded()) {
                promise.handle((AsyncResult<Void>) resultMap.remove(key));
            } else {
                promise.fail(ar.cause());
            }
        });
    }
}
