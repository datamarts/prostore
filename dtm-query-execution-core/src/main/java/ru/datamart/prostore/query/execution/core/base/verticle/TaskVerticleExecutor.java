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
package ru.datamart.prostore.query.execution.core.base.verticle;

import ru.datamart.prostore.common.eventbus.DataTopic;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskVerticleExecutor extends AbstractVerticle {
    private final Map<String, Handler<Promise>> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();
    private final VertxPoolProperties vertxPoolProperties;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions options = new DeploymentOptions()
                .setInstances(vertxPoolProperties.getTaskPool());
        vertx.deployVerticle(() -> new TaskVerticle(taskMap, resultMap), options, ar -> {
            if (ar.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }

    public <T> Future<T> execute(Handler<Promise<T>> codeHandler) {
        return Future.future(promise -> {
            String taskId = UUID.randomUUID().toString();
            taskMap.put(taskId, (Handler) codeHandler);
            vertx.eventBus().request(
                    DataTopic.START_WORKER_TASK.getValue(),
                    taskId,
                    new DeliveryOptions().setSendTimeout(vertxPoolProperties.getTaskTimeout()),
                    ar -> {
                        taskMap.remove(taskId);
                        if (ar.succeeded()) {
                            promise.handle((AsyncResult<T>) resultMap.remove(ar.result().body().toString()));
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
