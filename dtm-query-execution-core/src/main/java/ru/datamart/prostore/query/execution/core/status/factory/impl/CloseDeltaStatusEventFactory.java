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
package ru.datamart.prostore.query.execution.core.status.factory.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.common.status.StatusEventKey;
import ru.datamart.prostore.common.status.delta.CloseDeltaEvent;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaRecord;
import ru.datamart.prostore.query.execution.core.status.factory.AbstractStatusEventFactory;

import static ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;

@Component
public class CloseDeltaStatusEventFactory extends AbstractStatusEventFactory<DeltaRecord, CloseDeltaEvent> {

    @Autowired
    protected CloseDeltaStatusEventFactory() {
        super(DeltaRecord.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DELTA_CLOSE;
    }

    @Override
    protected CloseDeltaEvent createEventMessage(StatusEventKey eventKey, DeltaRecord eventData) {
        return new CloseDeltaEvent(eventData.getDeltaNum(), eventData.getDeltaDate().format(DELTA_DATE_TIME_FORMATTER));
    }
}
