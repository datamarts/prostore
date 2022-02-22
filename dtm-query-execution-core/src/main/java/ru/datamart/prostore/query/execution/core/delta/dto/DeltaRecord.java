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
package ru.datamart.prostore.query.execution.core.delta.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import ru.datamart.prostore.common.delta.DeltaLoadStatus;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.WriteOpFinish;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DeltaRecord {
    private String datamart;
    private LocalDateTime deltaDate;
    private Long deltaNum;
    private Long cnFrom;
    private Long cnTo;
    private Long cnMax;
    private boolean rollingBack;
    private List<WriteOpFinish> writeOperationsFinished;
    private DeltaLoadStatus status;
}
