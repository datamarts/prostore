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
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.delta.dto.query.ResumeWriteOperationDeltaQuery;
import ru.datamart.prostore.query.execution.core.edml.dto.EraseWriteOpResult;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ResumeWriteOperationServiceTest {

    @Mock
    private RestoreStateService restoreStateService;

    @InjectMocks
    private ResumeWriteOperationService service;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testExecuteWithEraseOpsResumed() {
        List<EraseWriteOpResult> eraseResult = Arrays.asList(new EraseWriteOpResult("t1", 1L));
        when(restoreStateService.restoreErase("datamart", 1L)).thenReturn(Future.succeededFuture(eraseResult));
        when(restoreStateService.restoreUpload("datamart", 1L)).thenReturn(Future.succeededFuture(Collections.emptyList()));

        ResumeWriteOperationDeltaQuery query = new ResumeWriteOperationDeltaQuery(
                null, "datamart", null, null, 1L
        );

        service.execute(query).onComplete(res -> {
            if (res.failed()) {
                fail("Resume write operations failed: " + res.cause());
            }
        });
    }

    @Test
    public void testExecuteWithUploadOpsResumed() {
        List<QueryResult> uploadResult = Arrays.asList(QueryResult.emptyResult());
        when(restoreStateService.restoreErase("datamart", 1L)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(restoreStateService.restoreUpload("datamart", 1L)).thenReturn(Future.succeededFuture(uploadResult));

        ResumeWriteOperationDeltaQuery query = new ResumeWriteOperationDeltaQuery(
                null, "datamart", null, null, 1L
        );

        service.execute(query).onComplete(res -> {
            if (res.failed()) {
                fail("Resume write operations failed: " + res.cause());
            }
        });
    }

    @Test
    public void testExecuteWithNoOpsResumed() {
        when(restoreStateService.restoreErase("datamart", 1L)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(restoreStateService.restoreUpload("datamart", 1L)).thenReturn(Future.succeededFuture(Collections.emptyList()));

        ResumeWriteOperationDeltaQuery query = new ResumeWriteOperationDeltaQuery(
                null, "datamart", null, null, 1L
        );

        service.execute(query).onComplete(result -> {
            if (result.succeeded()) {
                fail("Should have failed because there are no running operations");
            } else {
                assertEquals("Write operation not found", result.cause().getMessage());
            }
        });
    }

    @Test
    public void testExecuteWithEraseFailed() {
        when(restoreStateService.restoreUpload("datamart", 1L)).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(restoreStateService.restoreErase("datamart", 1L)).thenReturn(Future.failedFuture(new DtmException("Erase failed")));

        ResumeWriteOperationDeltaQuery query = new ResumeWriteOperationDeltaQuery(
                null, "datamart", null, null, 1L
        );

        service.execute(query).onComplete(result -> {
            if (result.succeeded()) {
                fail("Should have failed because erase is failed");
            } else {
                assertEquals("Erase failed", result.cause().getMessage());
            }
        });
    }

}
