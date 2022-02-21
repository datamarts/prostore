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
package ru.datamart.prostore.query.execution.core.rollback.dto;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import org.apache.calcite.sql.SqlNode;

public class RollbackRequestContext extends CoreRequestContext<RollbackRequest, SqlNode> {

    public RollbackRequestContext(RequestMetrics metrics,
                                  String envName,
                                  RollbackRequest request,
                                  SqlNode sqlNode) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
