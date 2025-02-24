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
package ru.datamart.prostore.query.execution.core.metrics.service;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.query.execution.core.metrics.factory.MetricsEventFactory;
import io.vertx.core.eventbus.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MetricsConsumer {

    private final MetricsEventFactory<RequestMetrics> eventFactory;
    private final MetricsProcessingService operationMetricsService;

    @Autowired
    public MetricsConsumer(MetricsEventFactory<RequestMetrics> eventFactory,
                           MetricsProcessingService operationMetricsService) {
        this.operationMetricsService = operationMetricsService;
        this.eventFactory = eventFactory;
    }

    public void consume(Message<String> message) {
        final RequestMetrics requestMetrics = eventFactory.create(message.body());
        operationMetricsService.process(requestMetrics);
    }
}
