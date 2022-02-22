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
package ru.datamart.prostore.query.execution.core.status.verticle;

import ru.datamart.prostore.common.eventbus.DataHeader;
import ru.datamart.prostore.common.eventbus.DataTopic;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaStatusEventPublisher;
import ru.datamart.prostore.query.execution.core.status.service.StatusEventFactoryRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@ConditionalOnProperty(
        value = "core.kafka.status.event.publish.enabled",
        havingValue = "true"
)
@Component
public class StatusEventVerticle extends AbstractVerticle {

    private final KafkaStatusEventPublisher kafkaStatusEventPublisher;
    private final StatusEventFactoryRegistry statusEventFactoryRegistry;

    @Autowired
    public StatusEventVerticle(KafkaStatusEventPublisher kafkaStatusEventPublisher,
                               StatusEventFactoryRegistry statusEventFactoryRegistry) {
        this.kafkaStatusEventPublisher = kafkaStatusEventPublisher;
        this.statusEventFactoryRegistry = statusEventFactoryRegistry;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(DataTopic.STATUS_EVENT_PUBLISH.getValue(), this::onPublishStatusEvent);
    }

    private void onPublishStatusEvent(Message<String> statusMessage) {
        try {
            val eventCode = StatusEventCode.valueOf(statusMessage.headers()
                    .get(DataHeader.STATUS_EVENT_CODE.getValue()));
            val datamart = statusMessage.headers().get(DataHeader.DATAMART.getValue());
            val eventRequest = statusEventFactoryRegistry.get(eventCode)
                    .create(datamart, statusMessage.body());
            kafkaStatusEventPublisher.publish(eventRequest)
                    .onFailure(err -> log.error("StatusEvent publish error", err));
        } catch (Exception e) {
            log.error("Error generating status event request", e);
        }
    }
}
