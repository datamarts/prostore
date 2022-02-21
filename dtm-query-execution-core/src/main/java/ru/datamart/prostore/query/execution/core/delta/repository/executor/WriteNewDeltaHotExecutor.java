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
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaIsNotCommittedException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNumIsNotNextToActualException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class WriteNewDeltaHotExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    @Autowired
    public WriteNewDeltaHotExecutor(ZookeeperExecutor executor,
                                    @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<Long> execute(String datamart, Long deltaHotNum) {
        val deltaStat = new Stat();
        Promise<Long> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
                .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
                .map(delta -> getDeltaToWrite(delta, deltaHotNum))
                .compose(delta -> executor
                        .multi(getWriteNewDeltaHot(datamart, delta, deltaStat.getVersion()))
                        .map(r -> delta))
                .onSuccess(delta -> {
                    log.debug("Write new delta hot by datamart[{}] completed successfully: [{}]", datamart, delta.getHot());
                    resultPromise.complete(delta.getHot().getDeltaNum());
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't write new delta hot on datamart[%s], deltaHotNumber[%d]",
                            datamart,
                            deltaHotNum);
                    if (error instanceof KeeperException.NodeExistsException
                            || error instanceof KeeperException.BadVersionException) {
                        resultPromise.fail(deltaHotNum == null ?
                                new DeltaIsNotCommittedException(error) :
                                new DeltaIsNotCommittedException(deltaHotNum, error));
                    } else if (error instanceof DeltaException) {
                        resultPromise.fail(error);
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                });

        return resultPromise.future();
    }

    private Delta getDeltaToWrite(Delta delta, Long deltaHotNum) {
        if (delta.getHot() != null) {
            throw new DeltaIsNotCommittedException(delta.getHot().getDeltaNum());
        }
        var deltaNum = 0L;
        var cnFrom = 0L;
        if (delta.getOk() != null) {
            deltaNum = delta.getOk().getDeltaNum() + 1;
            cnFrom = delta.getOk().getCnTo() + 1;
        }
        if (deltaHotNum != null && deltaHotNum != deltaNum) {
            throw new DeltaNumIsNotNextToActualException(deltaHotNum.toString());
        }
        val hotDelta = HotDelta.builder()
                .deltaNum(deltaNum)
                .cnFrom(cnFrom)
                .cnMax(cnFrom - 1)
                .rollingBack(false)
                .build();
        return delta.toBuilder()
                .hot(hotDelta)
                .build();
    }

    private Iterable<Op> getWriteNewDeltaHot(String datamart,
                                             Delta delta,
                                             int deltaVersion) {
        return Arrays.asList(
                createDatamartNodeOp(getDatamartPath(datamart), "/run"),
                createDatamartNodeOp(getDatamartPath(datamart), "/block"),
                Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteNewDeltaHotExecutor.class;
    }
}
