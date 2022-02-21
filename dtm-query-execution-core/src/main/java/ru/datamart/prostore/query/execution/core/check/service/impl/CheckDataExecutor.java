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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckData;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.table.ColumnsNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaNotExistException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service("checkDataExecutor")
public class CheckDataExecutor implements CheckExecutor {
    private final DataSourcePluginService pluginService;
    private final DeltaServiceDao deltaDao;
    private final EntityDao entityDao;
    private final CheckQueryResultFactory resultFactory;

    @Autowired
    public CheckDataExecutor(DataSourcePluginService pluginService,
                             DeltaServiceDao deltaDao,
                             EntityDao entityDao,
                             CheckQueryResultFactory resultFactory) {
        this.pluginService = pluginService;
        this.deltaDao = deltaDao;
        this.entityDao = entityDao;
        this.resultFactory = resultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        val sqlCheckData = (SqlCheckData) context.getSqlNode();
        val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val table = sqlCheckData.getTable();
        return entityDao.getEntity(datamart, table)
                .compose(entity -> validateEntity(entity, sqlCheckData.getColumns()))
                .compose(entity -> check(datamart, entity, sqlCheckData, context))
                .map(checkResults -> resultFactory.create(mapCheckResultToString(checkResults)));
    }

    private Future<Entity> validateEntity(Entity entity, Set<String> requestedColumns) {
        if (!EntityType.TABLE.equals(entity.getEntityType())) {
            throw new EntityNotExistsException(entity.getNameWithSchema());
        }
        val entityColumns = entity.getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());
        if (requestedColumns != null && !entityColumns.containsAll(requestedColumns)) {
            throw new ColumnsNotExistsException(requestedColumns.stream()
                    .filter(column -> !entityColumns.contains(column))
                    .collect(Collectors.joining(", ")));
        }
        return Future.succeededFuture(entity);
    }

    private Future<List<CheckResult>> check(String datamart, Entity entity, SqlCheckData sqlCheckData, CheckContext context) {
        return deltaDao.getDeltaOk(datamart)
                .map(deltaOk -> {
                    if (deltaOk == null) {
                        throw new DeltaNotExistException();
                    }
                    return deltaOk;
                })
                .compose(deltaOk -> checkDeltas(deltaOk.getDeltaNum(), datamart, entity, sqlCheckData, context));
    }

    private String mapCheckResultToString(List<CheckResult> checkResults) {
        val sortedResult = checkResults.stream()
                .sorted((c1, c2) -> Long.compare(c2.getDeltaNum(), c1.getDeltaNum()))
                .collect(Collectors.toList());
        List<String> result = new ArrayList<>();
        for (CheckResult checkResult : sortedResult) {
            result.add(checkResult.getResult());
            if (!checkResult.isSucceeded()) {
                break;
            }
        }
        return String.join("\n", result);
    }

    private Future<List<CheckResult>> checkDeltas(Long deltaOkNum, String datamart, Entity entity, SqlCheckData sqlCheckData, CheckContext context) {
        val deltaNum = sqlCheckData.getDeltaNum();
        if (deltaOkNum < deltaNum) {
            throw new DeltaNotExistException();
        }
        List<Future> checkList = new ArrayList<>();
        for (Long curDeltaNum = deltaOkNum; curDeltaNum >= deltaNum; curDeltaNum--) {
            checkList.add(checkCurrentDeltaNum(datamart, curDeltaNum, entity, sqlCheckData, context));
        }
        return CompositeFuture.join(checkList)
                .map(CompositeFuture::list);
    }

    private Future<CheckResult> checkCurrentDeltaNum(String datamart,
                                                     Long deltaNum,
                                                     Entity entity,
                                                     SqlCheckData sqlCheckData,
                                                     CheckContext context) {
        return deltaDao.getDeltaByNum(datamart, deltaNum)
                .compose(delta -> checkDeltaInPlugins(entity, sqlCheckData, context, delta));

    }

    private Future<CheckResult> checkDeltaInPlugins(Entity entity, SqlCheckData sqlCheckData, CheckContext context, OkDelta delta) {
        return CompositeFuture.join(entity.getDestination().stream()
                .map(sourceType -> getCheckFunc(entity, sqlCheckData, context)
                        .apply(sourceType, delta.getCnFrom(), delta.getCnTo())
                        .map(value -> new Pair<>(sourceType, value)))
                .collect(Collectors.toList()))
                .map(result -> {
                    List<Pair<SourceType, Long>> resultList = result.list();
                    if (resultList.stream().map(Pair::getValue).distinct().count() == 1) {
                        val checkResult = String.format("Table '%s.%s' (%s) checksum for delta %s is Ok.",
                                entity.getSchema(), entity.getName(),
                                entity.getDestination().stream()
                                        .map(SourceType::name)
                                        .sorted()
                                        .collect(Collectors.joining(", ")),
                                delta.getDeltaNum());
                        return new CheckResult(checkResult, true, delta.getDeltaNum());
                    } else {
                        val errorMessage = resultList.stream()
                                .map(pair -> String.format("%s: %s", pair.getKey(), pair.getValue().toString()))
                                .collect(Collectors.joining("\n"));
                        return new CheckResult(String.format("Table '%s.%s' checksum mismatch!%n%s",
                                entity.getSchema(), entity.getName(), errorMessage),
                                false,
                                delta.getDeltaNum());
                    }
                });
    }

    private TriFunction<SourceType, Long, Long, Future<Long>> getCheckFunc(Entity entity, SqlCheckData sqlCheckData, CheckContext context) {
        val normalization = sqlCheckData.getNormalization();
        val columns = sqlCheckData.getColumns();
        return (columns == null || columns.isEmpty()) ?
                (sourceType, cnFrom, cnTo) -> pluginService.checkDataByCount(sourceType,
                        context.getMetrics(),
                        CheckDataByCountRequest.builder()
                                .requestId(context.getRequest().getQueryRequest().getRequestId())
                                .envName(context.getEnvName())
                                .datamart(context.getRequest().getQueryRequest().getDatamartMnemonic())
                                .entity(entity)
                                .cnFrom(cnFrom)
                                .cnTo(cnTo)
                                .build())
                : getCheckHashInt32Func(context, entity, columns, normalization);
    }

    private TriFunction<SourceType, Long, Long, Future<Long>> getCheckHashInt32Func(CheckContext context,
                                                                                    Entity entity,
                                                                                    Set<String> columns,
                                                                                    Long normalization) {
        Set<String> entityFieldNames = entity.getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());
        Set<String> notExistColumns = columns.stream()
                .filter(column -> !entityFieldNames.contains(column))
                .collect(Collectors.toSet());

        if (!notExistColumns.isEmpty()) {
            throw new DtmException(String.format("Columns: `%s` don't exist.",
                    String.join(", ", notExistColumns)));
        }
        return (sourceType, cnFrom, cnTo) -> pluginService.checkDataByHashInt32(
                sourceType,
                context.getMetrics(),
                CheckDataByHashInt32Request.builder()
                        .requestId(context.getRequest().getQueryRequest().getRequestId())
                        .envName(context.getEnvName())
                        .datamart(context.getRequest().getQueryRequest().getDatamartMnemonic())
                        .entity(entity)
                        .cnFrom(cnFrom)
                        .cnTo(cnTo)
                        .columns(columns)
                        .normalization(normalization)
                        .build());

    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    @Getter
    @AllArgsConstructor
    private static class CheckResult {
        private final String result;
        private final boolean succeeded;
        private final Long deltaNum;
    }

    @FunctionalInterface
    private interface TriFunction<A, B, C, R> {
        R apply(A a, B b, C c);
    }
}
