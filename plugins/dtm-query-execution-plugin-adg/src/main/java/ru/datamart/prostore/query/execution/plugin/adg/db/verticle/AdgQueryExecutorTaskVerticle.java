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
package ru.datamart.prostore.query.execution.plugin.adg.db.verticle;

import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgClientProvider;
import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgResultTranslator;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class AdgQueryExecutorTaskVerticle extends AbstractVerticle {
    private final AdgClientProvider clientProvider;
    private final AdgResultTranslator resultTranslator;
    private final Map<String, AdgExecutorTask> taskMap;
    private final Map<String, AsyncResult<List<Object>>> resultMap;

    public AdgQueryExecutorTaskVerticle(AdgClientProvider clientProvider,
                                        AdgResultTranslator resultTranslator,
                                        Map<String, AdgExecutorTask> taskMap,
                                        Map<String, AsyncResult<List<Object>>> resultMap) {
        this.clientProvider = clientProvider;
        this.resultTranslator = resultTranslator;
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(AdgExecutorTopic.CALL_QUERY.getTopic(), this::executeCallQueryHandler);
    }

    private void executeCallQueryHandler(Message<String> message) {
        String key = message.body();
        AdgExecutorTask adgExecutorTask = taskMap.get(key);
        callQuery(adgExecutorTask.getFunction(), adgExecutorTask.getArgs())
                .onComplete(ar -> {
                    resultMap.put(key, ar);
                    message.reply(key);
                });
    }

    private Future<List<Object>> callQuery(String sql, Object[] params) {
        if (params == null || params.length == 0) {
            return call("query", sql);
        } else {
            return call("query", sql, params);
        }
    }

    private Future<List<Object>> call(String function, Object... args) {
        return vertx.executeBlocking(event ->
                clientProvider.getClient().composableAsyncOps().call(function, args)
                        .thenAccept(event::complete)
                        .exceptionally(e -> {
                            event.fail(new DataSourceException(e));
                            return null;
                        }), false)
                .map(resultTranslator::translate);
    }
}
