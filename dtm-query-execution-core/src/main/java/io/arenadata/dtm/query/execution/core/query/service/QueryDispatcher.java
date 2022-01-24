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
package io.arenadata.dtm.query.execution.core.query.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.base.dto.request.CoreRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class QueryDispatcher {

    private final Map<SqlProcessingType, DatamartExecutionService<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>>> serviceMap = new EnumMap<>(SqlProcessingType.class);

    @Autowired
    public QueryDispatcher(List<DatamartExecutionService<? extends CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>>> services) {
        for (DatamartExecutionService<? extends CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> es : services) {
            serviceMap.put(es.getSqlProcessingType(), (DatamartExecutionService<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>>) es);
        }
    }

    public Future<QueryResult> dispatch(CoreRequestContext<?, ?> context) {
        try {
            return serviceMap.get(context.getProcessingType()).execute(context);
        } catch (Exception e) {
            return Future.failedFuture(new DtmException("An error occurred while dispatching the request", e));
        }
    }
}
