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
package ru.datamart.prostore.query.execution.core.rollback.factory;

import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.rollback.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.core.rollback.dto.RollbackRequestContext;
import org.springframework.stereotype.Component;

@Component
public class RollbackRequestContextFactory {

    public RollbackRequestContext create(EdmlRequestContext context) {
        return new RollbackRequestContext(
                context.getMetrics(),
                context.getEnvName(),
                RollbackRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .datamart(context.getDestinationEntity().getSchema())
                .destinationTable(context.getDestinationEntity().getName())
                .sysCn(context.getSysCn())
                .entity(context.getDestinationEntity())
                .build(),
                context.getSqlNode());
    }
}
