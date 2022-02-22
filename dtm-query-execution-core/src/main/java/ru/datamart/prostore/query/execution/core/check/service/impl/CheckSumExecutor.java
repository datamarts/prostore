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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckSum;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.table.ColumnsNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.dto.CheckSumRequestContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaIsEmptyException;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

@Service("checkSumExecutor")
public class CheckSumExecutor implements CheckExecutor {

    private static final Set<EntityType> ALLOWED_ENTITY_TYPES = EnumSet.of(EntityType.TABLE, EntityType.MATERIALIZED_VIEW);
    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final CheckSumTableService checkSumTableService;
    private final CheckQueryResultFactory queryResultFactory;

    @Autowired
    public CheckSumExecutor(DeltaServiceDao deltaServiceDao,
                            EntityDao entityDao,
                            CheckSumTableService checkSumTableService,
                            CheckQueryResultFactory queryResultFactory) {
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.checkSumTableService = checkSumTableService;
        this.queryResultFactory = queryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(promise -> {
            val sqlCheckSum = (SqlCheckSum) context.getSqlNode();
            val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
            val deltaNum = sqlCheckSum.getDeltaNum();
            val normalization = sqlCheckSum.getNormalization();
            val table = sqlCheckSum.getTable();
            val columns = sqlCheckSum.getColumns();
            val checksumType = sqlCheckSum.getChecksumType();
            val checkContext = CheckSumRequestContext.builder()
                    .checkContext(context)
                    .deltaNum(deltaNum)
                    .datamart(datamart)
                    .columns(columns)
                    .normalization(normalization)
                    .checksumType(checksumType)
                    .build();
            deltaServiceDao.getDeltaHot(datamart)
                    .compose(hotDelta -> {
                        if (hotDelta == null || hotDelta.getDeltaNum() != deltaNum) {
                            return deltaServiceDao.getDeltaByNum(datamart, deltaNum)
                                    .compose(okDelta -> calculateCheckSum(table, checkContext, okDelta.getCnFrom(), okDelta.getCnTo()));
                        }
                        return calculateCheckSum(table, checkContext, hotDelta.getCnFrom(), hotDelta.getCnTo());
                    })
                    .map(this::createQueryResult)
                    .onComplete(promise);
        });
    }

    private Future<Long> calculateCheckSum(String table, CheckSumRequestContext checkContext, long cnFrom, Long cnTo) {
        if (cnTo == null) {
            return Future.failedFuture(new DeltaIsEmptyException(checkContext.getDeltaNum()));
        }
        checkContext.setCnFrom(cnFrom);
        checkContext.setCnTo(cnTo);
        if (table != null) {
            return entityDao.getEntity(checkContext.getDatamart(), table)
                    .compose(entity -> validateEntity(entity, checkContext))
                    .compose(entity -> checkSumTableService.calcCheckSumTable(checkContext));
        }
        return checkSumTableService.calcCheckSumForAllTables(checkContext);
    }

    private Future<Entity> validateEntity(Entity entity, CheckSumRequestContext checkContext) {
        if (!ALLOWED_ENTITY_TYPES.contains(entity.getEntityType())) {
            throw new EntityNotExistsException(entity.getName());
        }
        val entityColumns = entity.getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());
        val requestedColumns = checkContext.getColumns();
        if (requestedColumns != null && !entityColumns.containsAll(requestedColumns)) {
            throw new ColumnsNotExistsException(requestedColumns.stream()
                    .filter(column -> !entityColumns.contains(column))
                    .collect(Collectors.joining(", ")));
        }
        checkContext.setEntity(entity);
        return Future.succeededFuture(entity);
    }

    private QueryResult createQueryResult(Long sum) {
        return queryResultFactory.create(sum == null ? null : sum.toString());
    }

    @Override
    public CheckType getType() {
        return CheckType.SUM;
    }
}
