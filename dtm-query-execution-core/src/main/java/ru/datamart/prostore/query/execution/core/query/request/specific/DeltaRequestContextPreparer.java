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
package ru.datamart.prostore.query.execution.core.query.request.specific;

import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.calcite.core.extension.delta.SqlDeltaCall;
import ru.datamart.prostore.query.execution.core.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.DeltaRequestContext;

import static ru.datamart.prostore.query.execution.core.query.utils.MetricsUtils.createRequestMetrics;

@Component
public class DeltaRequestContextPreparer implements SpecificRequestContextPreparer {
    private final AppConfiguration coreConfiguration;

    public DeltaRequestContextPreparer(AppConfiguration coreConfiguration) {
        this.coreConfiguration = coreConfiguration;
    }

    @Override
    public Future<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> create(QueryRequest request, SqlNode node) {
        return Future.succeededFuture(
                new DeltaRequestContext(createRequestMetrics(request),
                        new DatamartRequest(request),
                        coreConfiguration.getEnvName(),
                        (SqlDeltaCall) node)
        );
    }

    @Override
    public boolean isApplicable(SqlNode node) {
        return node instanceof SqlDeltaCall;
    }
}
