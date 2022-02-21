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
package ru.datamart.prostore.query.execution.core.delta.service;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.dto.query.GetWriteOperationsDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class GetWriteOperationsServiceTest {

    @Mock
    private ServiceDbFacade serviceDbFacade;

    @Mock
    private DeltaServiceDao deltaServiceDao;

    private GetWriteOperationsService service;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        service = new GetWriteOperationsService(serviceDbFacade);
    }

    @Test
    public void testGetWriteOperationsSuccess(VertxTestContext testContext) {
        DeltaWriteOp op1 = DeltaWriteOp.builder()
                .sysCn(1L)
                .tableName("table")
                .tableNameExt("table_ext")
                .status(0)
                .query("SELECT * FROM table")
                .build();
        DeltaWriteOp op2 = DeltaWriteOp.builder()
                .sysCn(2L)
                .tableName("table2")
                .tableNameExt("table2_ext")
                .status(0)
                .query("SELECT * FROM table2")
                .build();

        when(deltaServiceDao.getDeltaWriteOperations("datamart"))
                .thenReturn(Future.succeededFuture(Arrays.asList(op1, op2)));
        service.execute(GetWriteOperationsDeltaQuery.builder().datamart("datamart").build())
                .onComplete(result -> {
                    if (result.failed()) {
                        testContext.failNow("Operation failed");
                    }
                    verifyMetadata(result.result().getMetadata());
                    verifyResult(result.result().getResult());
                    testContext.completeNow();
                });
    }

    private void verifyMetadata(List<ColumnMetadata> metadata) {
        assertEquals(ColumnType.BIGINT, metadata.get(0).getType());
        assertEquals(ColumnType.INT, metadata.get(1).getType());
        assertEquals(ColumnType.VARCHAR, metadata.get(2).getType());
        assertEquals(ColumnType.VARCHAR, metadata.get(3).getType());
        assertEquals(ColumnType.VARCHAR, metadata.get(4).getType());

        assertEquals(GetWriteOperationsService.SYS_CN_COLUMN, metadata.get(0).getName());
        assertEquals(GetWriteOperationsService.STATUS_COLUMN, metadata.get(1).getName());
        assertEquals(GetWriteOperationsService.DESTINATION_TABLE_NAME_COLUMN, metadata.get(2).getName());
        assertEquals(GetWriteOperationsService.EXTERNAL_TABLE_NAME_COLUMN, metadata.get(3).getName());
        assertEquals(GetWriteOperationsService.QUERY_COLUMN, metadata.get(4).getName());

        assertEquals(5, metadata.size());
    }

    private void verifyResult(List<Map<String, Object>> result) {
        assertEquals(2, result.size());

        Map<String, Object> resultSet1 = result.get(0);
        assertEquals(5, resultSet1.size());
        assertEquals("table", resultSet1.get(GetWriteOperationsService.DESTINATION_TABLE_NAME_COLUMN));
        assertEquals("table_ext", resultSet1.get(GetWriteOperationsService.EXTERNAL_TABLE_NAME_COLUMN));
        assertEquals(1L, resultSet1.get(GetWriteOperationsService.SYS_CN_COLUMN));
        assertEquals(0, resultSet1.get(GetWriteOperationsService.STATUS_COLUMN));
        assertEquals("SELECT * FROM table", resultSet1.get(GetWriteOperationsService.QUERY_COLUMN));

        Map<String, Object> resultSet2 = result.get(1);
        assertEquals(5, resultSet2.size());
        assertEquals("table2", resultSet2.get(GetWriteOperationsService.DESTINATION_TABLE_NAME_COLUMN));
        assertEquals("table2_ext", resultSet2.get(GetWriteOperationsService.EXTERNAL_TABLE_NAME_COLUMN));
        assertEquals(2L, resultSet2.get(GetWriteOperationsService.SYS_CN_COLUMN));
        assertEquals(0, resultSet2.get(GetWriteOperationsService.STATUS_COLUMN));
        assertEquals("SELECT * FROM table2", resultSet2.get(GetWriteOperationsService.QUERY_COLUMN));
    }

}
