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
package ru.datamart.prostore.query.execution.core.check.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.MatViewSyncProperties;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.query.utils.ExceptionUtils;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class CheckMaterializedViewResultFactory {
    protected static final String NAME_COLUMN = "name";
    protected static final String QUERY_COLUMN = "query";
    protected static final String SOURCE_COLUMN = "source";
    protected static final String DESTINATION_COLUMN = "destination";
    protected static final String LAST_SYNC_TIME_COLUMN = "last_sync_time";
    protected static final String LAST_SYNC_DELTA_COLUMN = "last_sync_delta";
    protected static final String LAST_SYNC_ERROR_COLUMN = "last_sync_error";
    protected static final String SYNC_NOW_COLUMN = "is_sync_now";
    protected static final String RETRIES_LEFT_COLUMN = "retries_left";
    protected static final String SYNC_PERIOD_COLUMN = "sync_period";
    protected static final String DATAMART_DELTA_OK_COLUMN = "datamart_delta_ok";
    private static final List<ColumnMetadata> METADATA = Arrays.asList(
            ColumnMetadata.builder().name(NAME_COLUMN).type(ColumnType.VARCHAR).nullable(false).build(),
            ColumnMetadata.builder().name(QUERY_COLUMN).type(ColumnType.VARCHAR).nullable(false).build(),
            ColumnMetadata.builder().name(SOURCE_COLUMN).type(ColumnType.VARCHAR).nullable(false).build(),
            ColumnMetadata.builder().name(DESTINATION_COLUMN).type(ColumnType.VARCHAR).nullable(false).build(),
            ColumnMetadata.builder().name(LAST_SYNC_TIME_COLUMN).type(ColumnType.TIMESTAMP).nullable(true).build(),
            ColumnMetadata.builder().name(LAST_SYNC_DELTA_COLUMN).type(ColumnType.BIGINT).nullable(true).build(),
            ColumnMetadata.builder().name(LAST_SYNC_ERROR_COLUMN).type(ColumnType.VARCHAR).nullable(true).build(),
            ColumnMetadata.builder().name(SYNC_NOW_COLUMN).type(ColumnType.BOOLEAN).nullable(false).build(),
            ColumnMetadata.builder().name(RETRIES_LEFT_COLUMN).type(ColumnType.BIGINT).nullable(false).build(),
            ColumnMetadata.builder().name(SYNC_PERIOD_COLUMN).type(ColumnType.BIGINT).nullable(false).build(),
            ColumnMetadata.builder().name(DATAMART_DELTA_OK_COLUMN).type(ColumnType.BIGINT).nullable(true).build()
    );

    private final MatViewSyncProperties matViewSyncProperties;

    public CheckMaterializedViewResultFactory(MatViewSyncProperties matViewSyncProperties) {
        this.matViewSyncProperties = matViewSyncProperties;
    }

    public QueryResult create(Long deltaNum, List<MatviewEntry> matviews) {
        val resultItems = matviews.stream()
                .map(matviewEntry -> prepareResultItem(deltaNum, matviewEntry))
                .collect(Collectors.toList());

        return QueryResult.builder()
                .metadata(METADATA)
                .result(resultItems)
                .build();
    }

    private Map<String, Object> prepareResultItem(Long deltaNum, MatviewEntry matviewEntry) {
        val entity = matviewEntry.entity;
        val cacheValue = matviewEntry.cacheValue;
        val destination = getDestinations(entity);
        val retriesLeft = matViewSyncProperties.getRetryCount() - cacheValue.getFailsCount();

        Map<String, Object> item = new HashMap<>();
        item.put(NAME_COLUMN, entity.getName());
        item.put(QUERY_COLUMN, entity.getViewQuery());
        item.put(SOURCE_COLUMN, entity.getMaterializedDataSource().name());
        item.put(DESTINATION_COLUMN, destination);
        item.put(LAST_SYNC_TIME_COLUMN, DateTimeUtils.toMicros(cacheValue.getLastSyncTime()));
        item.put(LAST_SYNC_DELTA_COLUMN, entity.getMaterializedDeltaNum());
        item.put(LAST_SYNC_ERROR_COLUMN, ExceptionUtils.prepareMessage(cacheValue.getLastSyncError()));
        item.put(SYNC_NOW_COLUMN, cacheValue.isInSync());
        item.put(RETRIES_LEFT_COLUMN, retriesLeft);
        item.put(SYNC_PERIOD_COLUMN, matViewSyncProperties.getPeriodMs());
        item.put(DATAMART_DELTA_OK_COLUMN, deltaNum);
        return item;
    }

    private String getDestinations(Entity entity) {
        return entity.getDestination().stream()
                .map(Enum::name)
                .sorted(String::compareTo)
                .collect(Collectors.joining(","));
    }

    @AllArgsConstructor
    @Getter
    public static class MatviewEntry {
        private final Entity entity;
        private final MaterializedViewCacheValue cacheValue;
    }
}
