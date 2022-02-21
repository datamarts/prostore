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
package ru.datamart.prostore.query.execution.core.dml.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.execution.core.base.service.DatamartExecutionService;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Service("coreDmlService")
public class DmlService implements DatamartExecutionService<DmlRequestContext> {
    private final Map<DmlType, DmlExecutor> executorMap;

    public DmlService() {
        this.executorMap = new EnumMap<>(DmlType.class);
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return getExecutor(context).execute(context);
    }

    private DmlExecutor getExecutor(DmlRequestContext context) {
        final DmlExecutor dmlExecutor = executorMap.get(context.getType());

        if (dmlExecutor != null) {
            return dmlExecutor;
        }

        throw new DtmException(
                String.format("Couldn't find dml executor for query kind %s",
                        context.getSqlNode().getKind()));
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DML;
    }

    public void addExecutor(DmlExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }
}
