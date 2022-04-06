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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.MppwFinishRequestHandler;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.RestLoadClient;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.service.mock.MockStatusReporter;
import ru.datamart.prostore.query.execution.plugin.adqm.status.dto.StatusReportDto;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class MppwFinishRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final String TEST_TOPIC = "adqm_topic";

    @Mock
    private RestLoadClient restLoadClient;

    @Mock
    private DatabaseExecutor executor;

    @Captor
    private ArgumentCaptor<String> queryCaptor;

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster("test_arenadata");
    }

    @Test
    void testFinishRequestCallOrder(VertxTestContext testContext) {
        // arrange
        when(executor.executeUpdate(queryCaptor.capture())).thenReturn(Future.succeededFuture());
        when(restLoadClient.stopLoading(any())).thenReturn(Future.succeededFuture());

        MockStatusReporter mockReporter = getMockReporter();
        val adqmCommonSqlFactory = new AdqmProcessingSqlFactory(ddlProperties, TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect());
        val handler = new MppwFinishRequestHandler(restLoadClient, executor,
                ddlProperties,
                mockReporter, adqmCommonSqlFactory);

        val request = MppwKafkaRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("shares")
                .envName("dev")
                .loadStart(true)
                .sysCn(101L)
                .destinationEntity(getEntity())
                .topic(TEST_TOPIC)
                .uploadMetadata(new BaseExternalEntityMetadata("", "", ExternalTableFormat.AVRO, ""))
                .build();

        // act
        handler.execute(request).onComplete(ar -> testContext.verify(() -> {
            // assert
            if (ar.failed()) {
                fail(ar.cause());
            }

            val expected = Arrays.asList(
                    Pattern.compile("DROP TABLE IF EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata"),
                    Pattern.compile("DROP TABLE IF EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata"),
                    Pattern.compile("DROP TABLE IF EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata"),
                    Pattern.compile("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_buffer"),
                    Pattern.compile("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                    Pattern.compile("INSERT INTO dev__shares.accounts_actual\n" +
                            "  SELECT column1, column2, column3, sys_from, 100, 1, '.+', arrayJoin\\(\\[-1, 1\\]\\)\n" +
                            "  FROM dev__shares.accounts_actual\n" +
                            "  WHERE sys_from < 101 AND sys_to > 101 AND \\(column1, column2\\) IN \\(\n" +
                            "    SELECT column1, column2\n" +
                            "    FROM dev__shares.accounts_buffer_shard\n" +
                            "  \\)"),
                    Pattern.compile("INSERT INTO dev__shares.accounts_actual \\(column1, column2, column3, sys_from, sys_to, sys_op, sys_close_date, sign\\)\n" +
                            "  SELECT column1, column2, column3, sys_from, 100, 0, '.+', arrayJoin\\(\\[-1, 1\\]\\)\n" +
                            "  FROM dev__shares.accounts_actual\n" +
                            "  WHERE sys_from < 101 AND sys_to > 101 AND \\(column1, column2\\) IN \\(\n" +
                            "    SELECT column1, column2\n" +
                            "    FROM dev__shares.accounts_actual_shard\n" +
                            "    WHERE sys_from = 101\n" +
                            "  \\)"),
                    Pattern.compile("SYSTEM FLUSH DISTRIBUTED dev__shares.accounts_actual"),
                    Pattern.compile("DROP TABLE IF EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata"),
                    Pattern.compile("DROP TABLE IF EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata"),
                    Pattern.compile("OPTIMIZE TABLE dev__shares.accounts_actual_shard ON CLUSTER test_arenadata FINAL")
            );

            for (int i = 0; i < expected.size(); i++) {
                Assertions.assertThat(queryCaptor.getAllValues().get(i)).containsPattern(expected.get(i));
            }

            assertTrue(mockReporter.wasCalled("finish"));

            verify(restLoadClient).stopLoading(any());
        }).completeNow());
    }

    @Test
    void testFinishRequestCallOrderWhenNullSysCn(VertxTestContext testContext) {
        // arrange
        when(executor.executeUpdate(queryCaptor.capture())).thenReturn(Future.succeededFuture());
        when(restLoadClient.stopLoading(any())).thenReturn(Future.succeededFuture());

        MockStatusReporter mockReporter = getMockReporter();
        val adqmCommonSqlFactory = new AdqmProcessingSqlFactory(ddlProperties, TestUtils.CALCITE_CONFIGURATION.adqmSqlDialect());
        val handler = new MppwFinishRequestHandler(restLoadClient, executor,
                ddlProperties,
                mockReporter, adqmCommonSqlFactory);

        val request = MppwKafkaRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("shares")
                .envName("dev")
                .loadStart(true)
                .sysCn(null)
                .destinationEntity(Entity.builder()
                        .externalTableLocationPath("standalone.table")
                        .build())
                .topic(TEST_TOPIC)
                .uploadMetadata(new BaseExternalEntityMetadata("", "", ExternalTableFormat.AVRO, ""))
                .build();

        // act
        handler.execute(request).onComplete(ar -> testContext.verify(() -> {
            // assert
            if (ar.failed()) {
                fail(ar.cause());
            }

            val expected = Arrays.asList(
                    Pattern.compile("SYSTEM FLUSH DISTRIBUTED standalone.table")
            );

            assertEquals(expected.size(), queryCaptor.getAllValues().size(), "Some invocations are not expected");
            for (int i = 0; i < expected.size(); i++) {
                Assertions.assertThat(queryCaptor.getAllValues().get(i)).containsPattern(expected.get(i));
            }

            assertTrue(mockReporter.wasCalled("finish"));
            verify(restLoadClient).stopLoading(any());
        }).completeNow());
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("accounts")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("column1")
                                .type(ColumnType.INT)
                                .ordinalPosition(1)
                                .primaryOrder(1)
                                .shardingOrder(1)
                                .build(),
                        EntityField.builder()
                                .name("column2")
                                .type(ColumnType.INT)
                                .ordinalPosition(2)
                                .primaryOrder(2)
                                .build(),
                        EntityField.builder()
                                .name("column3")
                                .type(ColumnType.VARCHAR)
                                .ordinalPosition(3)
                                .build()
                ))
                .build();
    }

    private Map<String, Object> createRowMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private MockStatusReporter getMockReporter() {
        Map<String, StatusReportDto> expected = new HashMap<>();
        expected.put("finish", new StatusReportDto(TEST_TOPIC));
        expected.put("error", new StatusReportDto(TEST_TOPIC));
        return new MockStatusReporter(expected);
    }
}
