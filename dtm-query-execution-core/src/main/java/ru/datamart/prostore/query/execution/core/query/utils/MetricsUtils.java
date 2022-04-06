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
package ru.datamart.prostore.query.execution.core.query.utils;

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.reader.QueryRequest;

import java.time.LocalDateTime;

public final class MetricsUtils {
    private MetricsUtils() {
    }

    public static RequestMetrics createRequestMetrics(QueryRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                .requestId(request.getRequestId())
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }
}
