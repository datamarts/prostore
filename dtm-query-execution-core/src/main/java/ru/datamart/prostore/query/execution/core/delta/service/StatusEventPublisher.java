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
package ru.datamart.prostore.query.execution.core.delta.service;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.SneakyThrows;
import lombok.val;
import ru.datamart.prostore.common.eventbus.DataHeader;
import ru.datamart.prostore.common.eventbus.DataTopic;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.serialization.CoreSerialization;

public interface StatusEventPublisher {

    @SneakyThrows
    default void publishStatus(StatusEventCode eventCode, String datamart, Object eventData) {
        val message = CoreSerialization.serializeAsString(eventData);
        val options = new DeliveryOptions();
        options.addHeader(DataHeader.DATAMART.getValue(), datamart);
        options.addHeader(DataHeader.STATUS_EVENT_CODE.getValue(), eventCode.name());
        getVertx().eventBus()
            .send(DataTopic.STATUS_EVENT_PUBLISH.getValue(), message, options);
    }

    Vertx getVertx();
}
