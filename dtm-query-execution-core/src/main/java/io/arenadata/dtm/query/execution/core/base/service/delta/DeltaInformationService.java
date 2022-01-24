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
package io.arenadata.dtm.query.execution.core.base.service.delta;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.exception.DeltaRangeInvalidException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class DeltaInformationService {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public DeltaInformationService(ServiceDbFacade serviceDbFacade) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    public Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime) {
        return Future.future(p -> deltaServiceDao.getDeltaByDateTime(datamart, dateTime)
                .onSuccess(delta -> p.complete(delta.getCnTo()))
                .onFailure(err -> p.complete(-1L)));
    }

    public Future<Long> getDeltaNumByDatetime(String datamart, LocalDateTime dateTime) {
        return Future.future(p -> deltaServiceDao.getDeltaByDateTime(datamart, dateTime)
                .onSuccess(delta -> p.complete(delta.getDeltaNum()))
                .onFailure(err -> p.complete(-1L)));
    }

    public Future<Long> getCnToByDeltaNum(String datamart, long num) {
        return Future.future(p -> deltaServiceDao.getDeltaByNum(datamart, num)
                .onSuccess(delta -> p.complete(delta.getCnTo()))
                .onFailure(err -> p.complete(-1L)));
    }

    public Future<Long> getCnToDeltaHot(String datamart) {
        return deltaServiceDao.getDeltaHot(datamart)
                .compose(deltaHot -> {
                    if (deltaHot != null && deltaHot.getCnTo() != null && deltaHot.getCnTo() >= 0) {
                        return Future.succeededFuture(deltaHot.getCnTo());
                    }

                    return handleDeltaOk(datamart);
                });
    }

    public Future<SelectOnInterval> getCnFromCnToByDeltaNums(String datamart, long deltaFrom, long deltaTo) {
        return Future.future(p -> CompositeFuture.join(deltaServiceDao.getDeltaByNum(datamart, deltaFrom), deltaServiceDao.getDeltaByNum(datamart, deltaTo))
                .onSuccess(ar -> {
                    Long cnFrom = ((OkDelta) ar.resultAt(0)).getCnFrom();
                    Long cnTo = ((OkDelta) ar.resultAt(1)).getCnTo();
                    p.complete(new SelectOnInterval(cnFrom, cnTo));
                })
                .onFailure(err -> {
                    val ex = new DeltaRangeInvalidException(String.format("Invalid delta range [%d, %d]", deltaFrom, deltaTo), err);
                    p.handle(Future.failedFuture(ex));
                }));
    }

    public Future<Long> getCnToDeltaOk(String datamart) {
        return handleDeltaOk(datamart);
    }

    private Future<Long> handleDeltaOk(String datamart) {
        return deltaServiceDao.getDeltaOk(datamart)
                .map(okDelta -> okDelta != null ? okDelta.getCnTo() : -1L);
    }
}
