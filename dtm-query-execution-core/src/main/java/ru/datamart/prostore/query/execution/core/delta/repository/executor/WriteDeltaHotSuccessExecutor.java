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

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.Delta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;

import static ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;

@Slf4j
@Component
public class WriteDeltaHotSuccessExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    private static final String CANT_WRITE_DELTA_HOT_MSG = "Can't write delta hot success";

    @Autowired
    public WriteDeltaHotSuccessExecutor(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<OkDelta> execute(String datamart, LocalDateTime deltaHotDate) {
        return Future.future(resultPromise -> {
            val deltaStat = new Stat();
            val ctx = new DeltaContext();
            val deltaDate = deltaHotDate != null ? deltaHotDate : LocalDateTime.now(CoreConstants.CORE_ZONE_ID).withNano(0);
            executor.getData(getDeltaPath(datamart), null, deltaStat)
                    .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
                    .map(delta -> {
                        if (delta.getHot() == null) {
                            throw new DeltaIsAlreadyCommittedException();
                        }
                        ctx.setDelta(delta);
                        return delta;
                    })
                    .compose(delta -> delta.getOk() == null ?
                            Future.succeededFuture(delta) : createDeltaPaths(datamart, deltaDate, delta))
                    .map(delta -> Delta.builder()
                            .ok(OkDelta.builder()
                                    .deltaDate(deltaDate)
                                    .deltaNum(delta.getHot().getDeltaNum())
                                    .cnFrom(delta.getHot().getCnFrom())
                                    .cnTo(delta.getHot().getCnTo() == null ? delta.getHot().getCnFrom() : delta.getHot().getCnTo())
                                    .build())
                            .build())
                    .compose(delta -> executor.multi(getWriteDeltaHotSuccessOps(datamart, delta, deltaStat.getVersion())).map(delta))
                    .onSuccess(delta -> {
                        log.debug("Write delta hot \"success\" by datamart[{}], deltaHotDate[{}] completed successfully",
                                datamart,
                                delta.getOk().getDeltaDate());
                        resultPromise.complete(delta.getOk());
                    })
                    .onFailure(error -> handleError(datamart, deltaHotDate, resultPromise, error));
        });
    }

    private void handleError(String datamart, LocalDateTime deltaHotDate, Promise<OkDelta> resultPromise, Throwable error) {
        val errMsg = String.format("Can't write delta hot \"success\" by datamart[%s], deltaDate[%s]",
                datamart,
                deltaHotDate);
        if (error instanceof KeeperException) {
            if (error instanceof KeeperException.NotEmptyException) {
                resultPromise.fail(new DeltaNotFinishedException(error));
            } else if (error instanceof KeeperException.BadVersionException) {
                resultPromise.fail(new DeltaAlreadyCommitedException(error));
            } else {
                resultPromise.fail(new DeltaException(errMsg, error));
            }
        } else if (error instanceof DeltaException) {
            resultPromise.fail(error);
        } else {
            resultPromise.fail(new DeltaException(errMsg, error));
        }
    }

    private Future<Delta> createDeltaPaths(String datamart, LocalDateTime deltaDate, Delta delta) {
        val deltaOkDate = delta.getOk().getDeltaDate();
        if (isBeforeOrEqual(deltaDate, deltaOkDate)) {
            return Future.failedFuture(
                    new DeltaUnableSetDateTimeException(DELTA_DATE_TIME_FORMATTER.format(deltaDate), DELTA_DATE_TIME_FORMATTER.format(deltaOkDate)));
        } else {
            return createDelta(datamart, delta);
        }
    }

    private boolean isBeforeOrEqual(LocalDateTime deltaHotDate, LocalDateTime actualOkDeltaDate) {
        return deltaHotDate.isBefore(actualOkDeltaDate) || deltaHotDate.isEqual(actualOkDeltaDate);
    }

    private Future<Delta> createDelta(String datamart, Delta delta) {
        return createDeltaDatePath(datamart, delta)
                .map(delta)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        return delta;
                    } else {
                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                    }
                })
                .compose(r ->
                        createDeltaDateTimePath(datamart, delta.getOk())
                                .map(delta)
                                .otherwise(error -> {
                                    if (error instanceof KeeperException.NodeExistsException) {
                                        return r;
                                    } else {
                                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                                    }
                                }))
                .compose(r ->
                        createDeltaDateNumPath(datamart, delta.getOk())
                                .map(delta)
                                .otherwise(error -> {
                                    if (error instanceof KeeperException.NodeExistsException) {
                                        return r;
                                    } else {
                                        throw new DeltaException(CANT_WRITE_DELTA_HOT_MSG, error);
                                    }
                                }));
    }

    private Future<String> createDeltaDatePath(String datamart, Delta delta) {
        val deltaDateTime = delta.getOk().getDeltaDate();
        val deltaDateTimePath = getDeltaDatePath(datamart, deltaDateTime.toLocalDate());
        return executor.createEmptyPersistentPath(deltaDateTimePath);
    }

    private Future<String> createDeltaDateTimePath(String datamart, OkDelta okDelta) {
        val deltaDateTime = okDelta.getDeltaDate();
        val deltaDateTimePath = getDeltaDateTimePath(datamart, deltaDateTime);
        return executor.createPersistentPath(deltaDateTimePath, serializedOkDelta(okDelta));
    }

    private Future<String> createDeltaDateNumPath(String datamart, OkDelta okDelta) {
        val deltaNum = okDelta.getDeltaNum();
        val deltaNumPath = getDeltaNumPath(datamart, deltaNum);
        return executor.createPersistentPath(deltaNumPath, serializedOkDelta(okDelta));
    }

    private Iterable<Op> getWriteDeltaHotSuccessOps(String datamart, Delta delta, int deltaVersion) {
        return Arrays.asList(
                Op.delete(getDatamartPath(datamart) + "/run", -1),
                Op.delete(getDatamartPath(datamart) + "/block", -1),
                Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteDeltaHotSuccessExecutor.class;
    }

}
