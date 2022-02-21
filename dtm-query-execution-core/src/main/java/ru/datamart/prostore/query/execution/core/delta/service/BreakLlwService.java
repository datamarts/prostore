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

import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BreakLlwService {

    private final DeltaServiceDao deltaServiceDao;

    public BreakLlwService(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    public Future<Void> breakLlw(String datamart) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream().filter(this::isLlwOperation).collect(Collectors.toList()))
                .compose(ops -> writeOperationsError(ops, datamart));
    }

    private boolean isLlwOperation(DeltaWriteOp op) {
        return op.getStatus() == WriteOperationStatus.EXECUTING.getValue() && op.getTableNameExt() == null;
    }

    private Future<Void> writeOperationsError(List<DeltaWriteOp> ops, String datamart) {
        List<Future> futures = new ArrayList<>(ops.size());
        ops.forEach(op -> futures.add(deltaServiceDao.writeOperationError(datamart, op.getSysCn())));

        return CompositeFuture.join(futures).mapEmpty();
    }

}
