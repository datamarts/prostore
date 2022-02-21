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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.service.DropTableExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final String ENV = "dev";
    private static final String DATAMART = "testdb";
    private static final String TABLE = "test_table";
    private static final String CLUSTER = "test_cluster";

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster(CLUSTER);
    }

    @Test
    public void testDropTable() {
        String dropTableActual = "DROP TABLE IF EXISTS %s__%s.%s_actual ON CLUSTER %s";
        String dropTableActualShard = "DROP TABLE IF EXISTS %s__%s.%s_actual_shard ON CLUSTER %s";
        MockDatabaseExecutor mockExecutor = new MockDatabaseExecutor(
                Arrays.asList(
                        s -> s.equalsIgnoreCase(String.format(dropTableActual, ENV, DATAMART, TABLE, CLUSTER)),
                        s -> s.equalsIgnoreCase(String.format(dropTableActualShard, ENV, DATAMART, TABLE, CLUSTER))
                ));
        DdlExecutor<Void> executor = new DropTableExecutor(mockExecutor, ddlProperties);

        Entity entity = new Entity(TABLE, DATAMART, Collections.emptyList());

        DdlRequest request = DdlRequest.builder()
                .envName(ENV)
                .entity(entity)
                .build();

        executor.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(mockExecutor.getExpectedCalls().size(),
                            mockExecutor.getCallCount(),
                            "All calls should be performed");
                });
    }
}
