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
package ru.datamart.prostore.query.execution.core.status.verticle;

import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaRecord;
import ru.datamart.prostore.query.execution.core.delta.dto.query.BeginDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.factory.impl.BeginDeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.service.BeginDeltaService;
import ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil;
import ru.datamart.prostore.query.execution.core.utils.QueryResultUtils;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class StatusEventVerticleTest {
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final DeltaQueryResultFactory deltaQueryResultFactory = mock(BeginDeltaQueryResultFactory.class);

    @BeforeEach
    void beforeAll() {
        delta.setDatamart(req.getDatamartMnemonic());
        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult());
        when(deltaQueryResultFactory.create(any())).thenReturn(queryResult);
        DeltaServiceDao deltaServiceDao = mock(DeltaServiceDao.class);
        when(deltaServiceDao.writeNewDeltaHot(any())).thenReturn(Future.succeededFuture(0L));
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void publishDeltaOpenEvent() {
        BeginDeltaService beginDeltaService =
                spy(new BeginDeltaService(serviceDbFacade, deltaQueryResultFactory, null, null));
        req.setSql("BEGIN DELTA");
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart("test")
                .request(req)
                .build();
        doNothing().when(beginDeltaService).publishStatus(any(), any(), any());
        when(beginDeltaService.getVertx()).thenReturn(null);
        beginDeltaService.execute(deltaQuery);
        verify(beginDeltaService, times(1)).publishStatus(eq(StatusEventCode.DELTA_OPEN),
                eq(deltaQuery.getDatamart()), any());
    }

    private List<Map<String, Object>> createResult() {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(0));
    }

}
