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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.exception.DdlDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;

import java.util.EnumMap;
import java.util.Map;

@Service("adbDdlService")
public class AdbDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new EnumMap<>(SqlKind.class);

    @Override
    @CacheEvict(value = AdbDtmDataSourcePlugin.ADB_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(DdlRequest request) {
        SqlKind sqlKind = request.getSqlKind();
        if (ddlExecutors.containsKey(sqlKind)) {
            return ddlExecutors.get(sqlKind)
                    .execute(request);
        } else {
            return Future.failedFuture(new DdlDatasourceException(String.format("Unknown DDL: %s", sqlKind)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
