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
package ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelRoot;
import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.delta.DeltaInformation;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
    private List<DeltaInformation> deltaInformations;
    private String envName;
    private CalciteContext calciteContext;
    private RelRoot relNode;
    private boolean isLocal = false;
    @Builder.Default
    private boolean allowStar = true;
}