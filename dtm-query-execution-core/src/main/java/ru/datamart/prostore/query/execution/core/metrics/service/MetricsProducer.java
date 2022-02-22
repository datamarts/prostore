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

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.metrics.MetricsEventCode;
import ru.datamart.prostore.common.metrics.MetricsHeader;
import ru.datamart.prostore.common.metrics.MetricsTopic;
import ru.datamart.prostore.serialization.CoreSerialization;

@Service
public class MetricsProducer {

    private final Vertx vertx;

    @Autowired
    public MetricsProducer(Vertx vertx) {
        this.vertx = vertx;
    }

    @SneakyThrows
    public void publish(MetricsTopic metricsTopic, Object value) {
        val message = CoreSerialization.serializeAsString(value);
        val options = new DeliveryOptions();
        options.addHeader(MetricsHeader.METRICS_EVENT_CODE.getValue(), MetricsEventCode.ALL.getValue());
        vertx.eventBus().request(metricsTopic.getValue(), message, options);
    }
}
