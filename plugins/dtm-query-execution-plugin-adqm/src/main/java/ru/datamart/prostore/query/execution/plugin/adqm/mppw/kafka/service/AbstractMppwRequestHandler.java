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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service;

import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.vertx.core.Future;
import lombok.NonNull;

import static java.lang.String.format;

public abstract class AbstractMppwRequestHandler implements KafkaMppwRequestHandler {
    private static final String DROP_TEMPLATE = "DROP TABLE IF EXISTS %s ON CLUSTER %s";

    protected final DatabaseExecutor databaseExecutor;
    protected final DdlProperties ddlProperties;

    protected AbstractMppwRequestHandler(DatabaseExecutor databaseExecutor, DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    protected Future<Void> dropTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(format(DROP_TEMPLATE, table, ddlProperties.getCluster()));
    }
}
