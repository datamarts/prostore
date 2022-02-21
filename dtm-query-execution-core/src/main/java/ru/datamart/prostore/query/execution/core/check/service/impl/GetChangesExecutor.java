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

import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class GetChangesExecutor implements CheckExecutor {

    private static final String OPERATION_NUM = "change_num";
    private static final String ENTITY_NAME = "entity_name";
    private static final String CHANGE_QUERY = "change_query";
    private static final String DATE_TIME_START = "date_time_start";
    private static final String DATE_TIME_END = "date_time_end";
    private static final String DELTA_NUM = "delta_num";
    private static final List<ColumnMetadata> METADATA = Arrays.asList(
            ColumnMetadata.builder().name(OPERATION_NUM).type(ColumnType.BIGINT).build(),
            ColumnMetadata.builder().name(ENTITY_NAME).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(CHANGE_QUERY).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(DATE_TIME_START).type(ColumnType.TIMESTAMP).build(),
            ColumnMetadata.builder().name(DATE_TIME_END).type(ColumnType.TIMESTAMP).build(),
            ColumnMetadata.builder().name(DELTA_NUM).type(ColumnType.BIGINT).build()
    );

    private final DatamartDao datamartDao;
    private final ChangelogDao changelogDao;

    @Autowired
    public GetChangesExecutor(DatamartDao datamartDao,
                              ChangelogDao changelogDao) {
        this.datamartDao = datamartDao;
        this.changelogDao = changelogDao;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        val datamart = context.getSqlNode().getSchema() == null ?
                context.getRequest().getQueryRequest().getDatamartMnemonic() :
                context.getSqlNode().getSchema();
        return datamartDao.existsDatamart(datamart)
                .compose(datamartExist -> datamartExist ? changelogDao.getChanges(datamart) : Future.failedFuture(new DatamartNotExistsException(datamart)))
                .map(list -> QueryResult.builder()
                        .result(list.stream()
                                .sorted(Comparator.comparing(Changelog::getOperationNumber))
                                .map(this::mapToResult)
                                .collect(Collectors.toList()))
                        .metadata(METADATA)
                        .build());
    }

    private Map<String, Object> mapToResult(Changelog changelog) {
        Map<String, Object> result = new HashMap<>();
        result.put(OPERATION_NUM, changelog.getOperationNumber());
        result.put(ENTITY_NAME, changelog.getEntityName());
        result.put(CHANGE_QUERY, changelog.getChangeQuery());
        result.put(DATE_TIME_START, DateTimeUtils.toMicros(CalciteUtil.parseLocalDateTime(changelog.getDateTimeStart())));
        result.put(DATE_TIME_END, changelog.getDateTimeEnd() == null ? null :
                DateTimeUtils.toMicros(CalciteUtil.parseLocalDateTime(changelog.getDateTimeEnd())));
        result.put(DELTA_NUM, changelog.getDeltaNum());
        return result;
    }

    @Override
    public CheckType getType() {
        return CheckType.CHANGES;
    }
}
