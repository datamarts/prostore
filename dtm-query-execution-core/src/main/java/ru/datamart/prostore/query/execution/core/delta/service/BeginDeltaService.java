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

import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.status.StatusEventCode;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.DeltaRecord;
import ru.datamart.prostore.query.execution.core.delta.dto.query.BeginDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction.BEGIN_DELTA;

@Component
@Slf4j
public class BeginDeltaService implements DeltaService, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating begin delta result";
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;
    private final Vertx vertx;

    @Autowired
    public BeginDeltaService(ServiceDbFacade serviceDbFacade,
                             @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                             @Qualifier("coreVertx") Vertx vertx,
                             EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return beginDelta(deltaQuery);
    }

    private Future<QueryResult> beginDelta(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val beginDeltaQuery = (BeginDeltaQuery) deltaQuery;
            if (beginDeltaQuery.getDeltaNum() == null) {
                writeDeltaHot(beginDeltaQuery).onComplete(promise);
            } else {
                writeDeltaHotByNum(beginDeltaQuery).onComplete(promise);
            }
        });
    }

    private Future<QueryResult> writeDeltaHotByNum(BeginDeltaQuery beginDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(beginDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart(), beginDeltaQuery.getDeltaNum())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            promise.complete(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<QueryResult> writeDeltaHot(BeginDeltaQuery beginDeltaQuery) {
        return Future.future(promise -> {
            try {
                evictQueryTemplateCacheService.evictByDatamartName(beginDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeNewDeltaHot(beginDeltaQuery.getDatamart())
                    .onSuccess(newDeltaHotNum -> {
                        try {
                            promise.complete(getDeltaQueryResult(newDeltaHotNum,
                                    beginDeltaQuery));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private QueryResult getDeltaQueryResult(Long deltaHotNum, BeginDeltaQuery deltaQuery) {
        val deltaRecord = createDeltaRecord(deltaQuery.getDatamart(), deltaHotNum);
        publishStatus(StatusEventCode.DELTA_OPEN, deltaQuery.getDatamart(), deltaRecord);
        val res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(deltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, Long deltaNum) {
        return DeltaRecord.builder()
                .deltaNum(deltaNum)
                .datamart(datamart)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return BEGIN_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
