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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.service;

import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class CreateSchemaExecutorTest {

    @Mock
    private DdlService<Void> ddlService;

    private final CreateSchemaExecutor createSchemaExecutor = new CreateSchemaExecutor();

    @Test
    void shouldSuccessWhenExecute(VertxTestContext testContext) {
        //act
        createSchemaExecutor.execute(DdlRequest.builder().build())
                .onComplete(ar -> testContext.verify(() ->
                        //assert
                        assertTrue(ar.succeeded()))
                        .completeNow());

        assertEquals(SqlKind.CREATE_SCHEMA, createSchemaExecutor.getSqlKind());
    }

    @Test
    void shouldSuccessWhenRegister() {
        //act
        createSchemaExecutor.register(ddlService);

        //assert
        verify(ddlService).addExecutor(createSchemaExecutor);
    }
}
