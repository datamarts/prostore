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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl;

import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.service.CreateDatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.service.DropDatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final String ENV = "dev";
    private static final String DATAMART = "testdb";
    private static final String CLUSTER = "test_cluster";

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster(CLUSTER);
    }

    @Test
    public void testCreateDatabase() {
        String createTemplate = "CREATE DATABASE IF NOT EXISTS %s__%s ON CLUSTER %s";
        DdlRequest request = DdlRequest.builder()
                .envName(ENV)
                .datamartMnemonic(DATAMART)
                .build();

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase(String.format(createTemplate, ENV, DATAMART, CLUSTER))));

        DdlExecutor<Void> databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties);

        databaseDdlService.execute(request).onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    public void testDropDatabase() {
        String dropTemplate = "DROP DATABASE IF EXISTS %s__%s ON CLUSTER %s";
        DdlRequest request = DdlRequest.builder()
                .envName(ENV)
                .datamartMnemonic(DATAMART)
                .build();

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase(String.format(dropTemplate, ENV, DATAMART, CLUSTER))));

        DdlExecutor<Void> databaseDdlService = new DropDatabaseExecutor(executor, ddlProperties);

        databaseDdlService.execute(request).onComplete(ar -> assertTrue(ar.succeeded()));
    }
}
