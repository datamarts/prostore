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

import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotExistException;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class GetDeltaByDateTimeExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    @Autowired
    public GetDeltaByDateTimeExecutor(ZookeeperExecutor executor,
                                      @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<OkDelta> execute(String datamart, LocalDateTime dateTime) {
        val ctx = new DeltaContext();
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
                .map(bytes -> {
                    val delta = deserializedDelta(bytes);
                    if (delta.getOk() != null) {
                        ctx.setDelta(delta);
                        val deltaDateTime = delta.getOk().getDeltaDate();
                        return deltaDateTime.isBefore(dateTime) || deltaDateTime.isEqual(dateTime);
                    } else {
                        throw new DeltaNotExistException();
                    }
                })
                .compose(isDeltaOk -> isDeltaOk ?
                        Future.succeededFuture(ctx.getDelta().getOk())
                        : findByDays(datamart, dateTime))
                .onSuccess(r -> {
                    log.debug("get delta ok by datamart[{}], dateTime[{}] completed successfully: [{}]", datamart, dateTime, r);
                    resultPromise.complete(r);
                })
                .onFailure(error -> {
                    val errMsg = String.format("can't get delta ok on datamart[%s], dateTime[%s]",
                            datamart,
                            dateTime);
                    if (error instanceof KeeperException.NoNodeException) {
                        resultPromise.fail(new DatamartNotExistsException(datamart));
                    } else if (error instanceof DeltaException) {
                        resultPromise.fail(error);
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                });
        return resultPromise.future();
    }

    private Future<OkDelta> findByDays(String datamart, LocalDateTime targetDateTime) {
        val date = targetDateTime.toLocalDate();
        return getDatamartDeltaDays(datamart, date)
                .compose(days -> {
                    if (days.isEmpty()) {
                        return Future.failedFuture(new DeltaNotFoundException());
                    } else {
                        val dayIterator = days.iterator();
                        return getDeltaOkByMaxDeltaDateTime(datamart, dayIterator.next(), targetDateTime)
                                .compose(okDelta -> {
                                    if (okDelta != null) {
                                        return Future.succeededFuture(okDelta);
                                    } else {
                                        if (dayIterator.hasNext()) {
                                            return getDeltaOkByMaxDeltaDateTime(datamart, dayIterator.next());
                                        } else {
                                            return Future.failedFuture(new DeltaNotFoundException());
                                        }
                                    }
                                });
                    }
                })
                .compose(okDelta -> okDelta == null ?
                        Future.failedFuture(new DeltaNotFoundException()) : Future.succeededFuture(okDelta));
    }

    private Future<List<LocalDate>> getDatamartDeltaDays(String datamart, LocalDate date) {
        return executor.getChildren(getDeltaPath(datamart) + "/date")
                .map(daysStr -> daysStr.stream()
                        .map(LocalDate::parse)
                        .filter(day -> date.isAfter(day) || date.isEqual(day))
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList()));
    }

    private Future<OkDelta> getDeltaOkByMaxDeltaDateTime(String datamart,
                                                         LocalDate day,
                                                         LocalDateTime targetDateTime) {
        return executor.getChildren(getDeltaDatePath(datamart, day))
                .map(times -> times.stream()
                        .map(LocalTime::parse)
                        .map(time -> LocalDateTime.of(day, time))
                        .filter(dateTime -> targetDateTime.isAfter(dateTime) || targetDateTime.equals(dateTime))
                        .max(Comparator.naturalOrder()))
                .compose(dateTimeOpt -> dateTimeOpt
                        .map(dateTime -> executor.getData(getDeltaDateTimePath(datamart, dateTime)))
                        .orElse(Future.succeededFuture()))
                .map(this::getOkDelta);
    }

    private Future<OkDelta> getDeltaOkByMaxDeltaDateTime(String datamart,
                                                         LocalDate day) {
        return executor.getChildren(getDeltaDatePath(datamart, day))
                .map(times -> times.stream()
                        .map(LocalTime::parse)
                        .map(time -> LocalDateTime.of(day, time))
                        .max(Comparator.naturalOrder()))
                .compose(dateTimeOpt -> dateTimeOpt
                        .map(dateTime -> executor.getData(getDeltaDateTimePath(datamart, dateTime)))
                        .orElse(Future.succeededFuture()))
                .map(this::getOkDelta);
    }

    private OkDelta getOkDelta(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            return deserializedOkDelta(bytes);
        }
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaByDateTimeExecutor.class;
    }
}
