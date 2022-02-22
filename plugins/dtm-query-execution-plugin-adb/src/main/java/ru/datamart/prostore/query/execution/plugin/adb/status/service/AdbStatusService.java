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
package ru.datamart.prostore.query.execution.plugin.adb.status.service;

import ru.datamart.prostore.common.plugin.status.StatusQueryResult;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.datamart.prostore.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbStatusService")
public class AdbStatusService implements StatusService {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;

    @Autowired
    public AdbStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic, String consumerGroup) {
        return kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, topic)
                .map(StatusQueryResult::new);
    }
}
