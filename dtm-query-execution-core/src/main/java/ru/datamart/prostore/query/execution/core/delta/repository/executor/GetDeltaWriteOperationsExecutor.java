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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaWriteOp;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class GetDeltaWriteOperationsExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    @Autowired
    public GetDeltaWriteOperationsExecutor(ZookeeperExecutor executor,
                                           @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<List<DeltaWriteOp>> execute(String datamart) {
        Promise<List<DeltaWriteOp>> resultPromise = Promise.promise();
        executor.exists(getDatamartPath(datamart) + "/run")
            .compose(isExists -> isExists ?
                executeIfExists(datamart, resultPromise) :
                Future.future(v -> resultPromise.complete(Collections.emptyList())));
        return resultPromise.future();
    }

    private Future<List<DeltaWriteOp>> executeIfExists(String datamart, Promise<List<DeltaWriteOp>> resultPromise) {
        return executor.getChildren(getDatamartPath(datamart) + "/run")
            .compose(opPaths -> getDeltaWriteOpList(datamart, opPaths))
            .onSuccess(writeOps -> {
                log.debug("Get delta write operations by datamart[{}] completed successfully: sysCn[{}]",
                    datamart, writeOps);
                resultPromise.complete(writeOps);
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't get delta write operation list by datamart[%s]",
                    datamart);
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.complete(Collections.emptyList());
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
    }

    private Future<List<DeltaWriteOp>> getDeltaWriteOpList(String datamart, List<String> opPaths) {
        return Future.future(promise -> {
            List<Long> opNums = opPaths.stream()
                .map(path -> Long.parseLong(path.substring(path.lastIndexOf("/") + 1)))
                .collect(Collectors.toList());
            CompositeFuture.join(opNums.stream()
                .map(opNum -> getDeltaWriteOp(datamart, opNum))
                .collect(Collectors.toList()))
                .onSuccess(ar -> promise.complete(ar.result().list().stream()
                    .map(wrOp -> (DeltaWriteOp) wrOp)
                    .collect(Collectors.toList())))
                .onFailure(promise::fail);
        });
    }

    private Future<DeltaWriteOp> getDeltaWriteOp(String datamart, Long opNum) {
        return executor.getData(getWriteOpPath(datamart, opNum))
            .map(this::deserializeDeltaWriteOp)
            .map(deltaWriteOp -> {
                deltaWriteOp.setSysCn(deltaWriteOp.getCnFrom() + opNum);
                return deltaWriteOp;
            });
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaWriteOperationsExecutor.class;
    }
}
