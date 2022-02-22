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
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.MppwStopReason;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.impl.BreakMppwContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BreakMppwService {

    private final DeltaServiceDao deltaServiceDao;
    private final long rollbackStatusCallsMs;
    private final Vertx vertx;

    public BreakMppwService(ServiceDbFacade serviceDbFacade,
                            RollbackDeltaProperties rollbackDeltaProperties,
                            Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.rollbackStatusCallsMs = rollbackDeltaProperties.getRollbackStatusCallsMs();
        this.vertx = vertx;
    }

    public Future<Void> breakMppw(String datamart) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream().filter(this::isMppwOperation).collect(Collectors.toList()))
                .compose(ops -> sendBreakMppwEvent(datamart, ops))
                .compose(ar -> waitUntilDone(datamart));
    }

    private boolean isMppwOperation(DeltaWriteOp op) {
        return op.getStatus() == WriteOperationStatus.EXECUTING.getValue() && op.getTableNameExt() != null;
    }

    private Future<Void> sendBreakMppwEvent(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(promise -> {
            ops.forEach(op -> {
                BreakMppwContext.requestRollback(datamart, op.getSysCn(), MppwStopReason.BREAK_MPPW_RECEIVED);
                log.debug("Stop MPPW [{}] task for [{}, {}] sent", MppwStopReason.BREAK_MPPW_RECEIVED, datamart, op.getSysCn());
            });
            promise.complete();
        });
    }

    private Future<Void> waitUntilDone(String datamart) {
        return Future.future(promise -> {
            if (BreakMppwContext.getNumberOfTasksByDatamart(datamart) == 0) {
                promise.complete();
                return;
            }

            periodicallyCheckTasks(datamart, promise);
        });
    }

    private void periodicallyCheckTasks(String datamart, Promise<Void> promise) {
        vertx.setTimer(rollbackStatusCallsMs, timerId -> {
            long count = BreakMppwContext.getNumberOfTasksByDatamart(datamart);
            log.debug("Break MPPW: Currently running {} tasks for datamart {}", count, datamart);
            if (count == 0) {
                promise.complete();
            } else {
                periodicallyCheckTasks(datamart, promise);
            }
        });
    }

}
