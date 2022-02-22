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
package ru.datamart.prostore.query.execution.core.check.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.execution.core.base.service.DatamartExecutionService;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import org.tarantool.util.StringUtils;

import java.util.EnumMap;
import java.util.Map;

@Service("coreCheckService")
public class CheckService implements DatamartExecutionService<CheckContext> {
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckService() {
        this.executorMap = new EnumMap<>(CheckType.class);
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (isDatamartRequired(context) && StringUtils.isEmpty(datamart)) {
            return Future.failedFuture(
                    new DtmException("Datamart must be specified for all tables and views"));
        } else {
            return executorMap.get(context.getCheckType())
                    .execute(context);
        }
    }

    private boolean isDatamartRequired(CheckContext context) {
        switch (context.getCheckType()) {
            case VERSIONS:
            case MATERIALIZED_VIEW:
            case CHANGES:
            case ENTITY_DDL:
                return false;
            default:
                return true;
        }
    }

    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CHECK;
    }

}
