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
package ru.datamart.prostore.query.execution.core.edml.mppw.factory;

import ru.datamart.prostore.query.execution.core.edml.mppw.dto.MppwStopFuture;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.MppwStopReason;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Component;

@Component
public class MppwErrorMessageFactory {
    private static final String OFFSET_RECEIVED_TEMPLATE = "Plugin: %s, status: %s, offset: %d";
    private static final String WITH_ERROR_REASON_TEMPLATE = "Plugin: %s, status: %s, offset: %d, stopReason: %s";
    private static final String WITH_ERROR_STOP_FUTURE_TEMPLATE = "Plugin: %s, status: %s, offset: %d, stopReason: %s, stopFailure: %s";

    public String create(MppwStopFuture stopFuture) {
        if (stopFuture.getFuture().failed()) {
            String stopFailure = getExceptionMessage(stopFuture.getFuture().cause());
            String stopReason = getExceptionMessage(stopFuture.getCause());
            return String.format(WITH_ERROR_STOP_FUTURE_TEMPLATE,
                    stopFuture.getSourceType().name(), stopFuture.getStopReason().name(),
                    stopFuture.getOffset() == null ? -1L : stopFuture.getOffset(),
                    stopReason,
                    stopFailure);
        } else if (MppwStopReason.OFFSET_RECEIVED != stopFuture.getStopReason()) {
            String stopReason = getExceptionMessage(stopFuture.getCause());
            return String.format(WITH_ERROR_REASON_TEMPLATE,
                    stopFuture.getSourceType().name(), stopFuture.getStopReason().name(),
                    stopFuture.getOffset() == null ? -1L : stopFuture.getOffset(),
                    stopReason);
        } else {
            return String.format(OFFSET_RECEIVED_TEMPLATE,
                    stopFuture.getSourceType().name(), stopFuture.getStopReason().name(),
                    stopFuture.getOffset() == null ? -1L : stopFuture.getOffset());
        }
    }

    public String getExceptionMessage(Throwable exception) {
        if (exception == null) {
            return "null";
        }

        Throwable mostSpecificCause = NestedExceptionUtils.getMostSpecificCause(exception);
        return mostSpecificCause.toString();
    }
}
