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
package ru.datamart.prostore.query.execution.core.base.dto.request;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.request.DatamartRequest;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

@Getter
@Setter
public abstract class CoreRequestContext<R extends DatamartRequest, S extends SqlNode> {
    protected final RequestMetrics metrics;
    protected final String envName;
    protected final R request;
    protected S sqlNode;

    protected CoreRequestContext(RequestMetrics metrics, String envName, R request, S sqlNode) {
        this.metrics = metrics;
        this.envName = envName;
        this.request = request;
        this.sqlNode = sqlNode;
    }

    public abstract SqlProcessingType getProcessingType();
}
