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
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.ResumeWriteOperationDeltaQuery;
import ru.datamart.prostore.query.execution.core.edml.dto.EraseWriteOpResult;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ResumeWriteOperationService implements DeltaService {

    private final RestoreStateService restoreStateService;

    public ResumeWriteOperationService(RestoreStateService restoreStateService) {
        this.restoreStateService = restoreStateService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        ResumeWriteOperationDeltaQuery query = (ResumeWriteOperationDeltaQuery) deltaQuery;

        List<QueryResult> uploadOps = new ArrayList<>();
        List<EraseWriteOpResult> eraseOps = new ArrayList<>();

        return restoreStateService.restoreUpload(query.getDatamart(), query.getSysCn())
                .onSuccess(upload -> uploadOps.addAll(upload))
                .compose(v -> restoreStateService.restoreErase(query.getDatamart(), query.getSysCn()))
                .onSuccess(erase -> eraseOps.addAll(erase))
                .compose(v -> checkEmpty(uploadOps, eraseOps))
                .map(QueryResult.emptyResult());
    }

    private Future<Void> checkEmpty(List<QueryResult> uploadOps, List<EraseWriteOpResult> eraseList) {
        if (uploadOps.isEmpty() && eraseList.isEmpty()) {
            throw new DtmException("Write operation not found");
        }
        return Future.succeededFuture();
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.RESUME_WRITE_OPERATION;
    }
}
