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
package ru.datamart.prostore.query.execution.core.delta.service;

import ru.datamart.prostore.query.execution.core.base.configuration.properties.RollbackDeltaProperties;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.service.BreakMppwService;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.BreakMppwContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class BreakMppwServiceTest {

    private static final String DATAMART = "test_datamart";

    private RollbackDeltaProperties rollbackDeltaProperties;

    @Mock
    private ServiceDbFacade serviceDbFacade;
    @Mock
    private DeltaServiceDao deltaServiceDao;

    private BreakMppwService executor;

    @BeforeEach
    public void setUp(Vertx vertx) {
        MockitoAnnotations.initMocks(this);

        DeltaWriteOp op1 = new DeltaWriteOp();
        op1.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op1.setSysCn(1L);
        op1.setTableNameExt("table_name_1_ext");

        DeltaWriteOp op2 = new DeltaWriteOp();
        op2.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op2.setSysCn(2L);
        op2.setTableNameExt("table_name_2_ext");

        DeltaWriteOp op3 = new DeltaWriteOp();
        op3.setStatus(WriteOperationStatus.SUCCESS.getValue());
        op3.setSysCn(3L);
        op3.setTableNameExt("table_name_3_ext");

        DeltaWriteOp op4 = new DeltaWriteOp();
        op4.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op4.setSysCn(2L);
        op4.setTableNameExt(null); // llw

        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(deltaServiceDao.getDeltaWriteOperations(DATAMART)).thenReturn(
                Future.succeededFuture(Arrays.asList(
                        op1, op2, op3, op4
                ))
        );

        rollbackDeltaProperties = new RollbackDeltaProperties();
        rollbackDeltaProperties.setRollbackStatusCallsMs(100);
        executor = new BreakMppwService(serviceDbFacade, rollbackDeltaProperties, vertx);
    }

    @Test
    public void testBreakMppw(VertxTestContext context, Vertx vertx) throws InterruptedException {
        vertx.setTimer(100, handler -> {
            if (BreakMppwContext.getNumberOfTasksByDatamart(DATAMART) != 2) {
                context.failNow("Wrong BREAK_MPPW tasks number in the queue");
            }
            BreakMppwContext.removeTask(DATAMART, 1);
            BreakMppwContext.removeTask(DATAMART, 2);
        });

        Future<Void> result = executor.breakMppw(DATAMART);

        Thread.sleep(150);

        result.onComplete(ar -> {
            if (ar.succeeded()) {
                context.completeNow();
            } else {
                context.failNow(ar.cause());
            }
        });
    }

}
