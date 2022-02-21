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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class DropSchemaExecutor implements DdlExecutor<Void> {

    private final DatabaseExecutor adbQueryExecutor;
    private final DdlSqlFactory sqlFactory;

    @Autowired
    public DropSchemaExecutor(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor, DdlSqlFactory sqlFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return createDropQuery(request.getDatamartMnemonic())
                .compose(adbQueryExecutor::executeUpdate);
    }

    private Future<String> createDropQuery(String datamartMnemonic) {
        return Future.future(promise -> {
            String dropSchemaSqlQuery = sqlFactory.dropSchemaSqlQuery(datamartMnemonic);
            promise.complete(dropSchemaSqlQuery);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
