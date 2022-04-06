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

import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.EraseWriteOperationDeltaQuery;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;

@Service
public class EraseWriteOperationService implements DeltaService {
    private final BreakMppwService breakMppwService;
    private final BreakLlwService breakLlwService;
    private final RestoreStateService restoreStateService;
    private final DatamartDao datamartDao;

    public EraseWriteOperationService(BreakMppwService breakMppwService,
                                      BreakLlwService breakLlwService,
                                      RestoreStateService restoreStateService,
                                      DatamartDao datamartDao) {
        this.breakMppwService = breakMppwService;
        this.breakLlwService = breakLlwService;
        this.restoreStateService = restoreStateService;
        this.datamartDao = datamartDao;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val query = (EraseWriteOperationDeltaQuery) deltaQuery;
            val datamart = query.getDatamart() == null ? query.getRequest().getDatamartMnemonic() : query.getDatamart();
            val sysCn = query.getSysCn();
            datamartDao.existsDatamart(datamart)
                    .compose(datamartExists -> datamartExists
                            ? breakMppwService.breakMppw(datamart, sysCn)
                            : Future.failedFuture(new DatamartNotExistsException(datamart)))
                    .compose(v -> breakLlwService.breakLlw(datamart, sysCn))
                    .compose(v -> restoreStateService.restoreErase(datamart, sysCn))
                    .map(v -> QueryResult.emptyResult())
                    .onComplete(promise);
        });
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.ERASE_WRITE_OPERATION;
    }
}
