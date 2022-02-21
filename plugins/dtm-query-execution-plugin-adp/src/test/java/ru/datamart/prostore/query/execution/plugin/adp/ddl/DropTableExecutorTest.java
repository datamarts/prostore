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
package ru.datamart.prostore.query.execution.plugin.adp.ddl;

import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.service.DropTableExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class DropTableExecutorTest {
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final DropTableExecutor dropTableExecutor = new DropTableExecutor(databaseExecutor);

    @BeforeEach
    void setUp() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testDropTable() {
        val expectedSql = "DROP TABLE IF EXISTS <dtm>.<tbl>_actual; DROP TABLE IF EXISTS <dtm>.<tbl>_history; DROP TABLE IF EXISTS <dtm>.<tbl>_staging; "
                .replace("<dtm>", SCHEMA)
                .replace("<tbl>", TABLE);
        val request= createDdlRequest();
        request.getEntity().setSchema(SCHEMA);
        request.getEntity().setName(TABLE);
        dropTableExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeUpdate(argThat(input -> input.equals(expectedSql)));
    }
}
