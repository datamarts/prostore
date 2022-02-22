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
package ru.datamart.prostore.query.execution.core.delta.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.service.DatamartExecutionService;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.DeltaRequestContext;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.dto.query.RollbackDeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryFactory;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.datamart.prostore.common.model.SqlProcessingType.DELTA;

@Service("coreDeltaService")
@Slf4j
public class DeltaExecutionService implements DatamartExecutionService<DeltaRequestContext> {

    private final Map<DeltaAction, DeltaService> executors;
    private final MetricsService metricsService;
    private final DeltaQueryFactory deltaQueryFactory;

    @Autowired
    public DeltaExecutionService(List<DeltaService> executors,
                                 @Qualifier("coreMetricsService") MetricsService metricsService,
                                 DeltaQueryFactory deltaQueryFactory) {
        this.executors = executors.stream()
                .collect(Collectors.toMap(DeltaService::getAction, Function.identity()));
        this.metricsService = metricsService;
        this.deltaQueryFactory = deltaQueryFactory;
    }

    @Override
    public Future<QueryResult> execute(DeltaRequestContext request) {
        if (StringUtils.isEmpty(request.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must be not empty!\n" +
                    "For setting datamart you can use the following command: \"USE datamartName\"";
            return Future.failedFuture(new DtmException(errMsg));
        } else {
            return extractDeltaAndExecute(request);
        }
    }

    private Future<QueryResult> extractDeltaAndExecute(DeltaRequestContext context) {
        return Future.future(promise -> createDeltaQuery(context)
                .compose(deltaQuery -> sendMetricsAndExecute(context, deltaQuery))
                .onComplete(deltaExecHandler -> {
                    if (deltaExecHandler.succeeded()) {
                        QueryResult queryDeltaResult = deltaExecHandler.result();
                        log.debug("Query result: {}, queryResult : {}",
                                context.getRequest().getQueryRequest(), queryDeltaResult);
                        promise.complete(queryDeltaResult);
                    } else {
                        promise.fail(deltaExecHandler.cause());
                    }
                }));
    }

    private Future<DeltaQuery> createDeltaQuery(DeltaRequestContext context) {
        return Future.future(promise -> promise.complete(deltaQueryFactory.create(context)));
    }

    private Future<QueryResult> sendMetricsAndExecute(DeltaRequestContext context, DeltaQuery deltaQuery) {
        deltaQuery.setDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic());
        deltaQuery.setRequest(context.getRequest().getQueryRequest());
        if (deltaQuery.getDeltaAction() != DeltaAction.ROLLBACK_DELTA) {
            return executeWithMetrics(context, deltaQuery);
        } else {
            final RollbackDeltaQuery rollbackDeltaQuery = (RollbackDeltaQuery) deltaQuery;
            rollbackDeltaQuery.setRequestMetrics(RequestMetrics.builder()
                    .requestId(context.getMetrics().getRequestId())
                    .startTime(context.getMetrics().getStartTime())
                    .status(RequestStatus.IN_PROCESS)
                    .isActive(true)
                    .build());
            return getExecutor(deltaQuery)
                    .compose(deltaExecutor -> execute(rollbackDeltaQuery));
        }
    }

    private Future<QueryResult> executeWithMetrics(DeltaRequestContext context, DeltaQuery deltaQuery) {
        return Future.future((Promise<QueryResult> promise) ->
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA, SqlProcessingType.DELTA, context.getMetrics())
                        .compose(result -> execute(deltaQuery))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.DELTA,
                                context.getMetrics(), promise)));
    }

    private Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return getExecutor(deltaQuery)
                .compose(deltaExecutor -> deltaExecutor.execute(deltaQuery));
    }

    private Future<DeltaService> getExecutor(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            final DeltaService executor = executors.get(deltaQuery.getDeltaAction());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException(String.format("Couldn't find delta executor for action %s",
                        deltaQuery.getDeltaAction())));
            }
        });
    }

    public SqlProcessingType getSqlProcessingType() {
        return DELTA;
    }
}
