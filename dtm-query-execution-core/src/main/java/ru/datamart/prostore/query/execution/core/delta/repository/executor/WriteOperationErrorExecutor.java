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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.CrashException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotExistException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaWriteOpNotFoundException;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.WriteOperationStatus;

@Slf4j
@Component
public class WriteOperationErrorExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    public WriteOperationErrorExecutor(ZookeeperExecutor executor,
                                       @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<Void> execute(String datamart, long sysCn) {
        Promise<Void> resultPromise = Promise.promise();
        val opNumCnCtx = new long[1];
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new CrashException("Delta hot not exists", new DeltaNotExistException());
                }
                opNumCnCtx[0] = sysCn - delta.getHot().getCnFrom();
                return opNumCnCtx[0];
            })
                .compose(opNum -> executor.getData(getWriteOpPath(datamart, opNum)))
                .map(this::deserializeDeltaWriteOp)
                .map(deltaWriteOp -> {
                    deltaWriteOp.setStatus(WriteOperationStatus.ERROR.getValue());
                    return deltaWriteOp;
                })
                .map(this::serializeDeltaWriteOp)
                .compose(deltaWriteOpData -> executor.setData(getWriteOpPath(datamart, opNumCnCtx[0]), deltaWriteOpData, -1))
                .onSuccess(ignored -> {
                    log.debug("Write delta operation \"error\" by datamart[{}], sysCn[{}] completed successfully", datamart, sysCn);
                    resultPromise.complete();
                })
            .onFailure(error -> {
                val errMsg = String.format("Can't write operation \"error\" on datamart[%s], sysCn[%d]",
                    datamart,
                    sysCn);
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.fail(new DeltaWriteOpNotFoundException(error));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteOperationErrorExecutor.class;
    }
}
