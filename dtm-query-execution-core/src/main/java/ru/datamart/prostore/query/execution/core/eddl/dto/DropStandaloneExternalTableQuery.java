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
package ru.datamart.prostore.query.execution.core.eddl.dto;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.datamart.prostore.common.metrics.RequestMetrics;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class DropStandaloneExternalTableQuery extends EddlQuery {

    private String envName;

    private Map<String, String> options;

    private RequestMetrics metrics;

    @Builder
    public DropStandaloneExternalTableQuery(EddlAction eddlAction, String schemaName, String tableName, String envName, Map<String, String> options, RequestMetrics metrics) {
        super(eddlAction, schemaName, tableName);
        this.envName = envName;
        this.options = options;
        this.metrics = metrics;
    }
}
