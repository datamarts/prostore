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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateDatabaseExecutor implements DdlExecutor<Void> {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE IF NOT EXISTS %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;

    public CreateDatabaseExecutor(DatabaseExecutor databaseExecutor,
                                  DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            createDatabase(request.getEnvName(), request.getDatamartMnemonic())
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createDatabase(String envName, String dbname) {
        String cluster = ddlProperties.getCluster();
        String createCmd = String.format(CREATE_TEMPLATE, envName, dbname, cluster);
        return databaseExecutor.executeUpdate(createCmd);
    }
}
