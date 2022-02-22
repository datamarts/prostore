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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpProperties;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.AdpQueryExecutor;

import java.util.Map;

public class AdpQueryExecutorTaskVerticle extends AbstractVerticle {
    private final String database;
    private final AdpProperties adpProperties;
    private final SqlTypeConverter fromAdpSqlTypeConverter;
    private final SqlTypeConverter toAdpSqlTypeConverter;
    private final Map<String, AdpExecutorTask> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;
    private AdpQueryExecutor adpQueryExecutor;

    public AdpQueryExecutorTaskVerticle(String database,
                                        AdpProperties adpProperties,
                                        SqlTypeConverter fromAdpSqlTypeConverter,
                                        SqlTypeConverter toAdpSqlTypeConverter,
                                        Map<String, AdpExecutorTask> taskMap,
                                        Map<String, AsyncResult<?>> resultMap) {
        this.database = database;
        this.adpProperties = adpProperties;
        this.fromAdpSqlTypeConverter = fromAdpSqlTypeConverter;
        this.toAdpSqlTypeConverter = toAdpSqlTypeConverter;
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        PgConnectOptions pgConnectOptions = new PgConnectOptions()
                .setDatabase(database)
                .setHost(adpProperties.getHost())
                .setPort(adpProperties.getPort())
                .setUser(adpProperties.getUser())
                .setPassword(adpProperties.getPassword())
                .setPreparedStatementCacheMaxSize(adpProperties.getPreparedStatementsCacheMaxSize())
                .setPreparedStatementCacheSqlLimit(adpProperties.getPreparedStatementsCacheSqlLimit())
                .setCachePreparedStatements(adpProperties.isPreparedStatementsCache())
                .setPipeliningLimit(1);

        PoolOptions poolOptions = new PoolOptions().setMaxSize(adpProperties.getPoolSize());
        PgPool pool = PgPool.pool(vertx, pgConnectOptions, poolOptions);

        adpQueryExecutor = new AdpQueryExecutor(pool, adpProperties.getFetchSize(), fromAdpSqlTypeConverter, toAdpSqlTypeConverter);

        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE.getTopic(), this::executeHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_WITH_CURSOR.getTopic(), this::executeWithCursorHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_WITH_PARAMS.getTopic(), this::executeWithParamsHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_UPDATE.getTopic(), this::executeUpdateHandler);
        vertx.eventBus().consumer(AdpExecutorTopic.EXECUTE_IN_TRANSACTION.getTopic(), this::executeInTransactionHandler);
    }

    private void executeHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.execute(adpExecutorTask.getSql(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });
    }

    private void executeWithCursorHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeWithCursor(adpExecutorTask.getSql(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeWithParamsHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeWithParams(adpExecutorTask.getSql(), adpExecutorTask.getParams(), adpExecutorTask.getMetadata())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeUpdateHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeUpdate(adpExecutorTask.getSql())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }

    private void executeInTransactionHandler(Message<String> message) {
        String key = message.body();
        AdpExecutorTask adpExecutorTask = taskMap.get(key);
        adpQueryExecutor.executeInTransaction(adpExecutorTask.getPreparedStatementRequests())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });

    }
}
