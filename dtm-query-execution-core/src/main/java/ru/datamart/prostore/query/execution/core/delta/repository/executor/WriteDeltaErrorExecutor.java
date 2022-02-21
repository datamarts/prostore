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

import ru.datamart.prostore.query.execution.core.aspect.RetryableFuture;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.exception.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class WriteDeltaErrorExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    @Autowired
    public WriteDeltaErrorExecutor(ZookeeperExecutor executor,
                                   @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @RetryableFuture(retries = 5, cause = KeeperException.BadVersionException.class)
    public Future<Void> execute(String datamart, Long deltaHotNum) {
        val deltaStat = new Stat();
        Promise<Void> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
                .map(bytes -> {
                    val delta = deserializedDelta(bytes);
                    if (delta.getHot() == null) {
                        throw new DeltaHotNotStartedException();
                    } else if (deltaHotNum != null && deltaHotNum != delta.getHot().getDeltaNum()) {
                        throw new DeltaNumIsNotNextToActualException(deltaHotNum.toString());
                    } else if (delta.getHot().isRollingBack()){
                        throw new DeltaAlreadyIsRollingBackException();
                    }
                    delta.getHot().setCnTo(-1L);
                    delta.getHot().setRollingBack(true);
                    return delta;
                })
                .compose(delta -> executor.multi(getErrorOps(datamart, delta, deltaStat.getVersion())))
                .onSuccess(r -> {
                    log.debug("Write delta error by datamart[{}], deltaNum[{}] completed successfully", datamart, deltaHotNum);
                    resultPromise.complete();
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't write delta error on datamart[%s], deltaNum[%s]",
                            datamart,
                            deltaHotNum);
                    if (error instanceof KeeperException) {
                        if (error instanceof KeeperException.NotEmptyException) {
                            resultPromise.fail(new DeltaNotFinishedException(error));
                        } else {
                            resultPromise.fail(new DeltaException(errMsg, error));
                        }
                    } else if (error instanceof DeltaException) {
                        resultPromise.fail(error);
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                });
        return resultPromise.future();
    }

    private Iterable<Op> getErrorOps(String datamart, Delta delta, int deltaVersion) {
        return Arrays.asList(
            Op.delete(getDatamartPath(datamart) + "/run", -1),
            Op.delete(getDatamartPath(datamart) + "/block", -1),
            Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteDeltaErrorExecutor.class;
    }
}
