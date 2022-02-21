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
package ru.datamart.prostore.query.execution.core.base.verticle;

import ru.datamart.prostore.common.eventbus.DataTopic;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;

@Slf4j
public class TaskVerticle extends AbstractVerticle {
    private final Map<String, Handler<Promise>> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;

    public TaskVerticle(Map<String, Handler<Promise>> taskMap, Map<String, AsyncResult<?>> resultMap) {
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(DataTopic.START_WORKER_TASK.getValue(), this::handle);
    }

    private void handle(Message<String> tMessage) {
        String requestId = tMessage.body();
        val task = taskMap.remove(requestId);
        Future.future(task::handle)
                .onComplete(ar -> {
                    resultMap.put(requestId, ar);
                    tMessage.reply(requestId);
                });
    }
}
