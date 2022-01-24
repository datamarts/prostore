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
package io.arenadata.dtm.query.execution.plugin.adg.status.service;

import io.arenadata.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdgStatusServiceTest {

    private static final String CONSUMER_GROUP = "consumer_group";
    private static final String TOPIC = "topic";
    @Mock
    private KafkaConsumerMonitor kafkaConsumerMonitor;

    @InjectMocks
    private AdgStatusService statusService;

    @Captor
    private ArgumentCaptor<String> consumerGroupCaptor;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Test
    void executeSuccess() {
        val kafkaPartitionInfo = KafkaPartitionInfo.builder()
                .consumerGroup(CONSUMER_GROUP)
                .topic(TOPIC)
                .build();
        when(kafkaConsumerMonitor.getAggregateGroupConsumerInfo(anyString(), anyString())).thenReturn(Future.succeededFuture(kafkaPartitionInfo));
        statusService.execute(TOPIC, CONSUMER_GROUP)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(CONSUMER_GROUP, ar.result().getPartitionInfo().getConsumerGroup());
                    assertEquals(TOPIC, ar.result().getPartitionInfo().getTopic());

                    verify(kafkaConsumerMonitor).getAggregateGroupConsumerInfo(consumerGroupCaptor.capture(), topicCaptor.capture());
                    assertEquals(CONSUMER_GROUP, consumerGroupCaptor.getValue());
                    assertEquals(TOPIC, topicCaptor.getValue());
                });
    }

    @Test
    void executeFail() {
        val errorMessage = "ERROR";
        when(kafkaConsumerMonitor.getAggregateGroupConsumerInfo(anyString(), anyString())).thenReturn(Future.failedFuture(errorMessage));
        statusService.execute(TOPIC, CONSUMER_GROUP)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(errorMessage, ar.cause().getMessage());
                });
    }
}
