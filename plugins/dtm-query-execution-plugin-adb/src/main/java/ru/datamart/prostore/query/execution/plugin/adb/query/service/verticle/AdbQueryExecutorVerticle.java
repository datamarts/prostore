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
package ru.datamart.prostore.query.execution.plugin.adb.query.service.verticle;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adb.base.configuration.properties.AdbProperties;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.pool.AdbConnectionFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service("adbQueryExecutor")
public class AdbQueryExecutorVerticle extends AbstractVerticle implements DatabaseExecutor {
    private static final DeliveryOptions DEFAULT_DELIVERY_OPTIONS = new DeliveryOptions()
            .setSendTimeout(86400000L);

    private final AdbProperties adbProperties;
    private final SqlTypeConverter fromAdbSqlTypeConverter;
    private final SqlTypeConverter toAdbSqlTypeConverter;
    private final AdbConnectionFactory connectionFactory;

    private final Map<String, AdbExecutorTask> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();

    public AdbQueryExecutorVerticle(AdbProperties adbProperties,
                                    @Qualifier("fromAdbSqlTypeConverter") SqlTypeConverter fromAdbSqlTypeConverter,
                                    @Qualifier("toAdbSqlTypeConverter") SqlTypeConverter toAdbSqlTypeConverter,
                                    AdbConnectionFactory connectionFactory) {
        this.adbProperties = adbProperties;
        this.fromAdbSqlTypeConverter = fromAdbSqlTypeConverter;
        this.toAdbSqlTypeConverter = toAdbSqlTypeConverter;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(adbProperties.getExecutorsCount());
        vertx.deployVerticle(() -> new AdbQueryExecutorTaskVerticle(adbProperties, fromAdbSqlTypeConverter, toAdbSqlTypeConverter, taskMap, resultMap, connectionFactory),
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
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE_WITH_CURSOR, request);
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters params, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .params(params)
                    .metadata(metadata)
                    .build();
            sendRequestWithResult(promise, AdbExecutorTopic.EXECUTE_WITH_PARAMS, request);
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .sql(sql)
                    .build();
            sendRequestWithoutResult(promise, AdbExecutorTopic.EXECUTE_UPDATE, request);
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        return Future.future(promise -> {
            AdbExecutorTask request = AdbExecutorTask.builder()
                    .preparedStatementRequests(requests)
                    .build();
            sendRequestWithoutResult(promise, AdbExecutorTopic.EXECUTE_IN_TRANSACTION, request);
        });
    }

    private void sendRequestWithResult(Promise<List<Map<String, Object>>> promise, AdbExecutorTopic topic, AdbExecutorTask request) {
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

    private void sendRequestWithoutResult(Promise<Void> promise, AdbExecutorTopic topic, AdbExecutorTask request) {
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
