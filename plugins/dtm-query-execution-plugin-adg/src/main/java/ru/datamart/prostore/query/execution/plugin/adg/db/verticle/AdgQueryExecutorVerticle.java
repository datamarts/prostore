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
package ru.datamart.prostore.query.execution.plugin.adg.db.verticle;

import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgClientProvider;
import ru.datamart.prostore.query.execution.plugin.adg.db.service.AdgResultTranslator;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AdgQueryExecutorVerticle extends AbstractVerticle {
    private static final DeliveryOptions DEFAULT_DELIVERY_OPTIONS = new DeliveryOptions()
            .setSendTimeout(86400000L);

    private final Map<String, AdgExecutorTask> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<List<Object>>> resultMap = new ConcurrentHashMap<>();
    private final TarantoolDatabaseProperties tarantoolDatabaseProperties;
    private final AdgClientProvider adgClientProvider;
    private final AdgResultTranslator resultTranslator;

    public AdgQueryExecutorVerticle(TarantoolDatabaseProperties tarantoolDatabaseProperties,
                                    AdgClientProvider adgClientProvider,
                                    AdgResultTranslator resultTranslator) {
        this.tarantoolDatabaseProperties = tarantoolDatabaseProperties;
        this.adgClientProvider = adgClientProvider;
        this.resultTranslator = resultTranslator;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(tarantoolDatabaseProperties.getVertxWorkers());
        deploymentOptions.setWorker(false);
        deploymentOptions.setWorkerPoolName("adgClientWorkerPool");
        deploymentOptions.setWorkerPoolSize(tarantoolDatabaseProperties.getVertxWorkers());

        vertx.deployVerticle(() -> new AdgQueryExecutorTaskVerticle(adgClientProvider, resultTranslator, taskMap, resultMap),
                deploymentOptions, ar -> {
                    if (ar.succeeded()) {
                        startPromise.complete();
                    } else {
                        startPromise.fail(ar.cause());
                    }
                });
    }

    public Future<List<Object>> callQuery(String sql, Object[] args) {
        return Future.future(promise -> {
            AdgExecutorTask request = AdgExecutorTask.builder()
                    .function(sql)
                    .args(args)
                    .build();
            sendRequestWithResult(promise, AdgExecutorTopic.CALL_QUERY, request);
        });
    }

    private void sendRequestWithResult(Promise<List<Object>> promise, AdgExecutorTopic topic, AdgExecutorTask request) {
        String key = UUID.randomUUID().toString();
        taskMap.put(key, request);
        vertx.eventBus().request(topic.getTopic(), key, DEFAULT_DELIVERY_OPTIONS, ar -> {
            taskMap.remove(key);
            val result = resultMap.remove(key);
            if (ar.succeeded()) {
                promise.handle(result);
            } else {
                promise.fail(ar.cause());
            }
        });
    }
}
