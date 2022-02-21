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
package ru.datamart.prostore.query.execution.core.config.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigType;
import ru.datamart.prostore.query.execution.core.base.service.DatamartExecutionService;
import ru.datamart.prostore.query.execution.core.config.dto.ConfigRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Service("coreConfigServiceImpl")
public class ConfigService implements DatamartExecutionService<ConfigRequestContext> {

    private final Map<SqlConfigType, ConfigExecutor> executorMap;

    @Autowired
    public ConfigService() {
        this.executorMap = new EnumMap<>(SqlConfigType.class);
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return getExecutor(context)
                .compose(executor -> executor.execute(context));
    }

    private Future<ConfigExecutor> getExecutor(ConfigRequestContext context) {
        return Future.future(promise -> {
            SqlConfigCall configCall = context.getSqlNode();
            ConfigExecutor executor = executorMap.get(configCall.getSqlConfigType());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException("Not supported config query type"));
            }
        });
    }

    public void addExecutor(ConfigExecutor executor) {
        executorMap.put(executor.getConfigType(), executor);
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.CONFIG;
    }
}
