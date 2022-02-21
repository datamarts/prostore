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
package ru.datamart.prostore.query.execution.core.metrics.verticle;

import ru.datamart.prostore.common.metrics.MetricsTopic;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsConsumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MetricsVerticle extends AbstractVerticle {

    private final MetricsConsumer consumer;

    @Autowired
    public MetricsVerticle(MetricsConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(MetricsTopic.ALL_EVENTS.getValue(), this::allEventsHandler);
    }

    private void allEventsHandler(Message<String> message) {
        consumer.consume(message);
    }
}
