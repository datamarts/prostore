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
package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.verticle.worker;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.handler.AdbMppwTransferDataHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;

@Slf4j
public class AdbMppwWorker extends AbstractVerticle {

    public static final int ERROR_CODE = 400;
    private final Map<String, MppwKafkaRequestContext> requestMap;
    private final Map<String, Future> resultMap;
    private final AdbMppwTransferDataHandler mppwTransferDataHandler;

    public AdbMppwWorker(Map<String, MppwKafkaRequestContext> requestMap, Map<String, Future> resultMap,
                         AdbMppwTransferDataHandler mppwTransferDataHandler) {
        this.requestMap = requestMap;
        this.resultMap = resultMap;
        this.mppwTransferDataHandler = mppwTransferDataHandler;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(MppwTopic.KAFKA_START.getValue(), this::handleStartMppwKafka);
        vertx.eventBus().consumer(MppwTopic.KAFKA_STOP.getValue(), this::handleStopMppwKafka);
        vertx.eventBus().consumer(MppwTopic.KAFKA_TRANSFER_DATA.getValue(), this::handleStartTransferData);
    }

    private void handleStartMppwKafka(Message<String> requestMessage) {
        val kafkaRequestContext = Json.decodeValue(requestMessage.body(), MppwKafkaRequestContext.class);
        log.debug("Received request for starting mppw kafka loading: {}", kafkaRequestContext);
        requestMap.put(kafkaRequestContext.getMppwKafkaLoadRequest().getRequestId(), kafkaRequestContext);
        vertx.eventBus().request(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                kafkaRequestContext.getMppwKafkaLoadRequest().getRequestId());
    }

    private void handleStopMppwKafka(Message<String> requestMessage) {
        val requestId = requestMessage.body();
        Future<?> transferPromise = resultMap.remove(requestId);
        if (transferPromise != null) {
            transferPromise.onComplete(ar -> {
                requestMap.remove(requestId);
                if (ar.failed()) {
                    requestMessage.fail(ERROR_CODE, ar.cause().getMessage());
                } else {
                    requestMessage.reply(requestId);
                }
            });
        } else {
            requestMessage.reply(requestId);
        }
    }

    private void handleStartTransferData(Message<String> requestMessage) {
        val requestId = requestMessage.body();
        val requestContext = requestMap.get(requestId);
        if (requestContext != null) {
            log.debug("Received requestId: {}, found requestContext in map: {}", requestId, requestContext);
            Promise<?> promise = Promise.promise();
            resultMap.put(requestId, promise.future());
            mppwTransferDataHandler.handle(requestContext)
                    .onSuccess(s -> {
                        log.debug("Request executed successfully: {}", requestContext.getMppwKafkaLoadRequest());
                        vertx.eventBus().request(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                                requestContext.getMppwKafkaLoadRequest().getRequestId());
                        promise.complete();
                    })
                    .onFailure(err -> {
                        log.error("Error transferring data: {}", requestContext.getMppwKafkaLoadRequest(), err);
                        promise.fail(err);
                    });
        }
    }
}
