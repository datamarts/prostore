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
package ru.datamart.prostore.query.execution.plugin.adp.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.factory.SchemaSqlFactory;
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

    private final DatabaseExecutor queryExecutor;
    private final SchemaSqlFactory sqlFactory;

    @Autowired
    public DropSchemaExecutor(DatabaseExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        this.sqlFactory = new SchemaSqlFactory();
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return queryExecutor.executeUpdate(sqlFactory.dropSchemaSqlQuery(request.getDatamartMnemonic()));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adpDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

}
