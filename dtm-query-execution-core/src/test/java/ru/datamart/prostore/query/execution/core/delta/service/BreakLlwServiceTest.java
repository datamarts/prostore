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

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class BreakLlwServiceTest {

    private static final String DATAMART = "test_datamart";

    @Mock
    private DeltaServiceDao deltaServiceDao;
    @Mock
    private ServiceDbFacade serviceDbFacade;

    private BreakLlwService breakLlwService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        DeltaWriteOp op1 = new DeltaWriteOp();
        op1.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op1.setSysCn(1L);
        op1.setTableNameExt("table_name_1_ext"); // mppw

        DeltaWriteOp op2 = new DeltaWriteOp();
        op2.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op2.setSysCn(2L);
        op2.setTableNameExt(null); // llw

        DeltaWriteOp op3 = new DeltaWriteOp();
        op3.setStatus(WriteOperationStatus.SUCCESS.getValue());
        op3.setSysCn(3L);
        op3.setTableNameExt(null); // llw

        DeltaWriteOp op4 = new DeltaWriteOp();
        op4.setStatus(WriteOperationStatus.EXECUTING.getValue());
        op4.setSysCn(4L);
        op4.setTableNameExt(null); // llw

        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(deltaServiceDao.getDeltaWriteOperations(DATAMART)).thenReturn(
                Future.succeededFuture(Arrays.asList(
                        op1, op2, op3, op4
                ))
        );
        when(deltaServiceDao.writeOperationError(DATAMART, 2)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeOperationError(DATAMART, 4)).thenReturn(Future.succeededFuture());

        breakLlwService = new BreakLlwService(serviceDbFacade);
    }

    @Test
    public void testBreakLlwSuccess(VertxTestContext testContext) {
        breakLlwService.breakLlw(DATAMART).onComplete(ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
            } else {
                verify(deltaServiceDao).writeOperationError(DATAMART, 2);
                verify(deltaServiceDao).writeOperationError(DATAMART, 4);
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 1);
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 3);
                testContext.completeNow();
            }
        });
    }

    @Test
    public void testBreakLlwRaisesExceptionWhenNoWriteOps(VertxTestContext testContext) {
        when(deltaServiceDao.getDeltaWriteOperations(DATAMART)).thenReturn(Future.failedFuture(new DtmException("")));
        breakLlwService.breakLlw(DATAMART).onComplete(ar -> {
            if (ar.failed()) {
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 1);
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 2);
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 3);
                verify(deltaServiceDao, never()).writeOperationError(DATAMART, 4);
                testContext.completeNow();
            } else {
                testContext.failNow("Should have been failed");
            }
        });
    }

    @Test
    public void testBreakLlwRaisesExceptionWhenFailedToWriteErrorOperation(VertxTestContext testContext) {
        when(deltaServiceDao.writeOperationError(DATAMART, 2)).thenReturn(Future.succeededFuture());
        when(deltaServiceDao.writeOperationError(DATAMART, 4)).thenReturn(Future.failedFuture(new DtmException("")));
        breakLlwService.breakLlw(DATAMART).onComplete(ar -> {
            if (ar.failed()) {
                testContext.completeNow();
            } else {
                testContext.failNow("Should have been failed");
            }
        });
    }

}
