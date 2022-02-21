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
package ru.datamart.prostore.query.execution.core.base.service.delta;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaType;
import ru.datamart.prostore.common.exception.DeltaRangeInvalidException;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.InformationSchemaView;
import ru.datamart.prostore.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DeltaQueryPreprocessor {
    private final DeltaInformationExtractor deltaInformationExtractor;
    private final DeltaInformationService deltaService;
    private final EntityDao entityDao;

    @Autowired
    public DeltaQueryPreprocessor(DeltaInformationService deltaService,
                                  DeltaInformationExtractor deltaInformationExtractor,
                                  EntityDao entityDao) {
        this.deltaService = deltaService;
        this.deltaInformationExtractor = deltaInformationExtractor;
        this.entityDao = entityDao;
    }

    public Future<DeltaQueryPreprocessorResponse> process(SqlNode request) {
        if (request == null) {
            log.error("Request is empty");
            return Future.failedFuture(new DtmException("Undefined request"));
        }

        return Future.future(p -> {
            val deltaInfoRes = deltaInformationExtractor.extract(request);
            calculateDeltaValues(deltaInfoRes.getDeltaInformations())
                    .map(deltaInformationList -> new DeltaQueryPreprocessorResponse(deltaInformationList, deltaInfoRes.getSqlWithoutSnapshots(), isCacheable(deltaInformationList)))
                    .onComplete(p);
        });
    }

    private Future<List<DeltaInformation>> calculateDeltaValues(List<DeltaInformation> deltas) {
        return Future.future(promise -> {
            val errors = new HashSet<String>();
            CompositeFuture.join(deltas.stream()
                            .map(deltaInformation -> getCalculateDeltaInfoFuture(deltaInformation)
                                    .onFailure(event -> errors.add(event.getMessage())))
                            .collect(Collectors.toList()))
                    .onSuccess(result -> promise.complete(result.list()))
                    .onFailure(error -> promise.fail(new DeltaRangeInvalidException(String.join(";", errors))));
        });
    }

    private Future<DeltaInformation> getCalculateDeltaInfoFuture(DeltaInformation deltaInformation) {
        if (InformationSchemaView.SCHEMA_NAME.equalsIgnoreCase(deltaInformation.getSchemaName())) {
            return Future.succeededFuture(deltaInformation);
        }

        if (deltaInformation.isLatestUncommittedDelta()) {
            return deltaService.getCnToDeltaHot(deltaInformation.getSchemaName())
                    .map(deltaCnTo -> {
                        deltaInformation.setSelectOnNum(deltaCnTo);
                        return deltaInformation;
                    });
        }

        if (DeltaType.FINISHED_IN.equals(deltaInformation.getType()) || DeltaType.STARTED_IN.equals(deltaInformation.getType())) {
            return getDeltaInformationFromInterval(deltaInformation);
        }

        return getDeltaInformationFromNum(deltaInformation);
    }

    private Future<DeltaInformation> getDeltaInformationFromInterval(DeltaInformation deltaInformation) {
        return Future.future(promise -> {
            val deltaFrom = deltaInformation.getSelectOnInterval().getSelectOnFrom();
            val deltaTo = deltaInformation.getSelectOnInterval().getSelectOnTo();
            deltaService.getCnFromCnToByDeltaNums(deltaInformation.getSchemaName(), deltaFrom, deltaTo)
                    .onSuccess(selectOnInterval -> {
                        deltaInformation.setSelectOnInterval(selectOnInterval);
                        promise.complete(deltaInformation);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<DeltaInformation> getDeltaInformationFromNum(DeltaInformation deltaInformation) {
        return Future.future(promise ->
                calculateSelectOnNum(deltaInformation)
                        .onSuccess(cnTo -> {
                            deltaInformation.setSelectOnNum(cnTo);
                            promise.complete(deltaInformation);
                        })
                        .onFailure(promise::fail));
    }

    private Future<Long> calculateSelectOnNum(DeltaInformation deltaInformation) {
        switch (deltaInformation.getType()) {
            case NUM:
                return deltaService.getCnToByDeltaNum(deltaInformation.getSchemaName(), deltaInformation.getSelectOnNum());
            case DATETIME:
                val localDateTime = deltaInformation.getDeltaTimestamp().replace("\'", "");
                return deltaService.getCnToByDeltaDatetime(deltaInformation.getSchemaName(), CalciteUtil.parseLocalDateTime(localDateTime));
            case WITHOUT_SNAPSHOT:
                return deltaService.getCnToDeltaOk(deltaInformation.getSchemaName())
                        .compose(deltaOkCnTo -> lowerDeltaForNotSyncedMatview(deltaInformation, deltaOkCnTo));
            default:
                return Future.failedFuture(new UnsupportedOperationException("Delta type not supported"));
        }
    }

    private Future<Long> lowerDeltaForNotSyncedMatview(DeltaInformation deltaInformation, Long deltaOkCnTo) {
        return entityDao.getEntity(deltaInformation.getSchemaName(), deltaInformation.getTableName())
                .compose(entity -> {
                    if (entity.getEntityType() != EntityType.MATERIALIZED_VIEW) {
                        return Future.succeededFuture(deltaOkCnTo);
                    }

                    val matviewDeltaNum = entity.getMaterializedDeltaNum();
                    if (matviewDeltaNum == null) {
                        return Future.succeededFuture(-1L);
                    }

                    return deltaService.getCnToByDeltaNum(entity.getSchema(), matviewDeltaNum)
                            .map(matviewCnTo -> Math.min(matviewCnTo, deltaOkCnTo));
                });
    }

    private boolean isCacheable(List<DeltaInformation> deltaInformationList) {
        return deltaInformationList.stream().noneMatch(DeltaInformation::isLatestUncommittedDelta);
    }
}
