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
package ru.datamart.prostore.query.execution.core.check.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.MatViewSyncProperties;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.query.utils.ExceptionUtils;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import static ru.datamart.prostore.query.execution.core.check.factory.CheckMaterializedViewResultFactory.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CheckMaterializedViewResultFactoryTest {
    private static final String DATAMART = "datamart";
    private static final String MATVIEW = "matview";
    private static final long DATAMART_DELTA_NUM = 2L;
    private static final long SYNC_DELTA_NUM = 1L;
    private static final String QUERY = "query";
    public static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 11, 11, 11);

    private final MatViewSyncProperties matViewSyncProperties = new MatViewSyncProperties();
    private final CheckMaterializedViewResultFactory factory = new CheckMaterializedViewResultFactory(matViewSyncProperties);

    @Test
    void shouldPrepareCorrectResultAndMetadata() {
        // arrange
        val entity = Entity.builder()
                .materializedDeltaNum(SYNC_DELTA_NUM)
                .schema(DATAMART)
                .name(MATVIEW)
                .viewQuery(QUERY)
                .materializedDataSource(SourceType.ADB)
                .destination(EnumSet.of(SourceType.ADQM, SourceType.ADG))
                .build();
        val cacheValue = new MaterializedViewCacheValue(entity);
        cacheValue.setLastSyncTime(DATE_TIME);
        cacheValue.incrementFailsCount();
        cacheValue.setInSync();
        val exception = new RuntimeException("Failure");
        cacheValue.setLastSyncError(exception);

        // act
        val queryResult = factory.create(DATAMART_DELTA_NUM, Collections.singletonList(new MatviewEntry(entity, cacheValue)));

        // assert
        assertEquals(1, queryResult.getResult().size());
        Map<String, Object> item = queryResult.getResult().get(0);
        assertEquals(MATVIEW, item.get(NAME_COLUMN));
        assertEquals(QUERY, item.get(QUERY_COLUMN));
        assertEquals("ADB", item.get(SOURCE_COLUMN));
        assertEquals("ADG,ADQM", item.get(DESTINATION_COLUMN));
        assertEquals(DateTimeUtils.toMicros(DATE_TIME), item.get(LAST_SYNC_TIME_COLUMN));
        assertEquals(SYNC_DELTA_NUM, item.get(LAST_SYNC_DELTA_COLUMN));
        assertEquals(ExceptionUtils.prepareMessage(exception), item.get(LAST_SYNC_ERROR_COLUMN));
        assertEquals(true, item.get(SYNC_NOW_COLUMN));
        assertEquals(matViewSyncProperties.getRetryCount() - 1L, item.get(RETRIES_LEFT_COLUMN));
        assertEquals(matViewSyncProperties.getPeriodMs(), item.get(SYNC_PERIOD_COLUMN));
        assertEquals(DATAMART_DELTA_NUM, item.get(DATAMART_DELTA_OK_COLUMN));

        assertThat(queryResult.getMetadata(), contains(
                allOf(
                        hasProperty("name", is(NAME_COLUMN)),
                        hasProperty("type", is(ColumnType.VARCHAR)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(QUERY_COLUMN)),
                        hasProperty("type", is(ColumnType.VARCHAR)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(SOURCE_COLUMN)),
                        hasProperty("type", is(ColumnType.VARCHAR)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(DESTINATION_COLUMN)),
                        hasProperty("type", is(ColumnType.VARCHAR)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(LAST_SYNC_TIME_COLUMN)),
                        hasProperty("type", is(ColumnType.TIMESTAMP)),
                        hasProperty("nullable", is(true))
                ),
                allOf(
                        hasProperty("name", is(LAST_SYNC_DELTA_COLUMN)),
                        hasProperty("type", is(ColumnType.BIGINT)),
                        hasProperty("nullable", is(true))
                ),
                allOf(
                        hasProperty("name", is(LAST_SYNC_ERROR_COLUMN)),
                        hasProperty("type", is(ColumnType.VARCHAR)),
                        hasProperty("nullable", is(true))
                ),
                allOf(
                        hasProperty("name", is(SYNC_NOW_COLUMN)),
                        hasProperty("type", is(ColumnType.BOOLEAN)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(RETRIES_LEFT_COLUMN)),
                        hasProperty("type", is(ColumnType.BIGINT)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(SYNC_PERIOD_COLUMN)),
                        hasProperty("type", is(ColumnType.BIGINT)),
                        hasProperty("nullable", is(false))
                ),
                allOf(
                        hasProperty("name", is(DATAMART_DELTA_OK_COLUMN)),
                        hasProperty("type", is(ColumnType.BIGINT)),
                        hasProperty("nullable", is(true))
                )
        ));
    }

}