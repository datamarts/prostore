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

import ru.datamart.prostore.common.model.ddl.Entity;
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

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.ACTUAL_POSTFIX;
import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.ACTUAL_SHARD_POSTFIX;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {
    private final static String DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS %s__%s.%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;

    @Autowired
    public DropTableExecutor(DatabaseExecutor databaseExecutor,
                             DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return dropTable(request.getEnvName(), request.getEntity());
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> dropTable(String envName, Entity entity) {

        String cluster = ddlProperties.getCluster();
        String schema = entity.getSchema();
        String table = entity.getName();

        String dropDistributed = String.format(DROP_TABLE_TEMPLATE, envName, schema, table + ACTUAL_POSTFIX, cluster);
        String dropShard = String.format(DROP_TABLE_TEMPLATE, envName, schema, table + ACTUAL_SHARD_POSTFIX, cluster);

        return databaseExecutor.executeUpdate(dropDistributed)
                .compose(v -> databaseExecutor.executeUpdate(dropShard));
    }

}
