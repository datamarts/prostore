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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adqm.base.service.AdqmDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.exception.DdlDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adqmDdlService")
@Slf4j
public class AdqmDdlService implements DdlService<Void> {
    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdqmDtmDataSourcePlugin.ADQM_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            SqlKind sqlKind = request.getSqlKind();
            if (ddlExecutors.containsKey(sqlKind)) {
                ddlExecutors.get(sqlKind)
                        .execute(request)
                        .onComplete(promise);
            } else {
                promise.fail(new DdlDatasourceException(String.format("Unknown DDL %s", sqlKind)));
            }
        });
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
