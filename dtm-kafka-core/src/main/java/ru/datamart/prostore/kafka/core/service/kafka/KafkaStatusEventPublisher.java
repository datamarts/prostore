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
package ru.datamart.prostore.kafka.core.service.kafka;

import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.status.PublishStatusEventRequest;
import ru.datamart.prostore.kafka.core.configuration.properties.PublishStatusEventProperties;
import ru.datamart.prostore.serialization.CoreSerialization;

@Slf4j
@ConditionalOnProperty(
        value = "core.kafka.status.event.publish.enabled",
        havingValue = "true"
)
@Service
public class KafkaStatusEventPublisher {
    private final KafkaProducer<String, String> producer;
    private final String eventTopic;

    public KafkaStatusEventPublisher(
            @Qualifier("jsonCoreKafkaProducer") KafkaProducer<String, String> producer,
            @Qualifier("publishStatusEventProperties") PublishStatusEventProperties properties) {
        this.producer = producer;
        this.eventTopic = properties.getTopic();
    }

    public Future<RecordMetadata> publish(PublishStatusEventRequest<?> request) {
        try {
            log.debug("Key [{}] and message [{}] sent to topic [{}]",
                    request.getEventKey(),
                    request.getEventMessage(),
                    eventTopic);
            val key = CoreSerialization.serializeAsString(request.getEventKey());
            val message = CoreSerialization.serializeAsString(request.getEventMessage());
            val producerRecord = KafkaProducerRecord.create(eventTopic, key, message);
            return producer.send(producerRecord);
        } catch (Exception ex) {
            return Future.failedFuture(new DtmException("Error creating status event record", ex));
        }
    }
}
