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
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.query.CommitDeltaQuery;
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

import static ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaService implements DeltaService, StatusEventPublisher {

    private static final String ERR_GETTING_QUERY_RESULT_MSG = "Error creating commit delta result";
    private final Vertx vertx;
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public CommitDeltaService(ServiceDbFacade serviceDbFacade,
                              @Qualifier("commitDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                              @Qualifier("coreVertx") Vertx vertx,
                              EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            val commitDeltaQuery = (CommitDeltaQuery) deltaQuery;
            try {
                evictQueryTemplateCacheService.evictByDatamartName(commitDeltaQuery.getDatamart());
            } catch (Exception e) {
                promise.fail(new DtmException("Evict cache error"));
            }
            deltaServiceDao.writeDeltaHotSuccess(commitDeltaQuery.getDatamart(), commitDeltaQuery.getDeltaDate())
                    .onSuccess(committedDeltaOk -> {
                        try {
                            promise.complete(getQueryResult(commitDeltaQuery, committedDeltaOk));
                        } catch (Exception e) {
                            promise.fail(new DtmException(ERR_GETTING_QUERY_RESULT_MSG, e));
                        }
                    })
                    .onFailure(promise::fail);
        });
    }

    private QueryResult getQueryResult(CommitDeltaQuery commitDeltaQuery, OkDelta okDelta) {
        val deltaRecord = createDeltaRecord(commitDeltaQuery.getDatamart(), okDelta);
        publishStatus(StatusEventCode.DELTA_CLOSE, commitDeltaQuery.getDatamart(), deltaRecord);
        val res = deltaQueryResultFactory.create(deltaRecord);
        res.setRequestId(commitDeltaQuery.getRequest().getRequestId());
        return res;
    }

    private DeltaRecord createDeltaRecord(String datamart, OkDelta okDelta) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(okDelta.getDeltaNum())
                .deltaDate(okDelta.getDeltaDate())
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return COMMIT_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
