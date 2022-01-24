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
package io.arenadata.dtm.query.execution.plugin.adqm.status.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adqm.status.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adqmStatusService")
@Slf4j
public class AdqmStatusService implements StatusService, StatusReporter {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final Map<String, String> topicsInUse = new HashMap<>();

    public AdqmStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic, String consumerGroup) {
        return Future.future(promise -> {
            if (topicsInUse.containsKey(topic)) {
                String consumerGroupInUse = topicsInUse.get(topic);
                kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroupInUse, topic)
                        .onSuccess(kafkaInfoResult -> {
                            StatusQueryResult result = new StatusQueryResult();
                            result.setPartitionInfo(kafkaInfoResult);
                            promise.complete(result);
                        })
                        .onFailure(promise::fail);
            } else {
                promise.fail(new DtmException(String.format("Topic %s is not processing now", topic)));
            }
        });
    }

    @Override
    public void onStart(@NonNull final StatusReportDto payload) {
        String topic = payload.getTopic();
        String consumerGroup = payload.getConsumerGroup();
        topicsInUse.put(topic, consumerGroup);
    }

    @Override
    public void onFinish(@NonNull final StatusReportDto payload) {
        topicsInUse.remove(payload.getTopic());
    }

    @Override
    public void onError(@NonNull final StatusReportDto payload) {
        topicsInUse.remove(payload.getTopic());
    }
}
