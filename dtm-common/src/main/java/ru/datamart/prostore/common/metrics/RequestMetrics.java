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
package ru.datamart.prostore.common.metrics;

import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RequestMetrics {
    private UUID requestId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private SqlProcessingType actionType;
    private SourceType sourceType;
    private RequestStatus status;
    private boolean isActive;
}
