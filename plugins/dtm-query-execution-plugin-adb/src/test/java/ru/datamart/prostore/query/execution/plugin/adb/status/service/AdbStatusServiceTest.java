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
package ru.datamart.prostore.query.execution.plugin.adb.status.service;

import ru.datamart.prostore.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbStatusServiceTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumer_group";

    @Mock
    private KafkaConsumerMonitor kafkaConsumerMonitor;

    @InjectMocks
    private AdbStatusService statusService;

    @Test
    void executeSuccess(VertxTestContext testContext) {
        val kafkaPartitionInfo = KafkaPartitionInfo.builder()
                .topic(TOPIC)
                .consumerGroup(CONSUMER_GROUP)
                .start(0L)
                .end(100L)
                .lag(40L)
                .offset(10L)
                .partition(1)
                .build();
        when(kafkaConsumerMonitor.getAggregateGroupConsumerInfo(CONSUMER_GROUP, TOPIC)).thenReturn(Future.succeededFuture(kafkaPartitionInfo));

        statusService.execute(TOPIC, CONSUMER_GROUP)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertThat(ar.result().getPartitionInfo()).isEqualToComparingFieldByField(kafkaPartitionInfo);
                }).completeNow());
    }

    @Test
    void executeGetAggregateGroupConsumerInfoError(VertxTestContext testContext) {
        when(kafkaConsumerMonitor.getAggregateGroupConsumerInfo(CONSUMER_GROUP, TOPIC)).thenReturn(Future.failedFuture("error"));

        statusService.execute(TOPIC, CONSUMER_GROUP)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals("error", ar.cause().getMessage());
                }).completeNow());
    }
}
