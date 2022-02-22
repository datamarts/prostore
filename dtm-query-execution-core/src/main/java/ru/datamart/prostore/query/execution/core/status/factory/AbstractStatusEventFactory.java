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
package ru.datamart.prostore.query.execution.core.status.factory;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.status.PublishStatusEventRequest;
import ru.datamart.prostore.common.status.StatusEventKey;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.time.LocalDateTime;
import java.util.UUID;

import static ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;

public abstract class AbstractStatusEventFactory<IN, OUT> implements StatusEventFactory<OUT> {
    private final Class<IN> inClass;

    protected AbstractStatusEventFactory(Class<IN> inClass) {
        this.inClass = inClass;
    }

    protected abstract OUT createEventMessage(StatusEventKey eventKey, IN eventData);

    @Override
    @SneakyThrows
    public PublishStatusEventRequest<OUT> create(@NonNull String datamart, @NonNull String eventData) {
        val eventKey = getEventKey(datamart);
        IN readValue = CoreSerialization.deserialize(eventData, inClass);
        return new PublishStatusEventRequest<>(eventKey, createEventMessage(eventKey, readValue));
    }

    @NotNull
    private StatusEventKey getEventKey(String datamart) {
        return new StatusEventKey(datamart,
                LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DELTA_DATE_TIME_FORMATTER),
                getEventCode(),
                UUID.randomUUID());
    }
}
