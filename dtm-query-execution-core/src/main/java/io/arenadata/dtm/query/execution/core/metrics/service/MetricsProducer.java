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
package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.common.metrics.MetricsEventCode;
import io.arenadata.dtm.common.metrics.MetricsHeader;
import io.arenadata.dtm.common.metrics.MetricsTopic;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetricsProducer {

    private final Vertx vertx;

    @Autowired
    public MetricsProducer(Vertx vertx) {
        this.vertx = vertx;
    }

    @SneakyThrows
    public void publish(MetricsTopic metricsTopic, Object value) {
        val message = DatabindCodec.mapper().writeValueAsString(value);
        val options = new DeliveryOptions();
        options.addHeader(MetricsHeader.METRICS_EVENT_CODE.getValue(), MetricsEventCode.ALL.getValue());
        vertx.eventBus().request(metricsTopic.getValue(), message, options);
    }
}
