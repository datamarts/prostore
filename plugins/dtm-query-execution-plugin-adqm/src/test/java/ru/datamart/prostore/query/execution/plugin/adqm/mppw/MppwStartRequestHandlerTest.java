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

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmTablesSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaLoadRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.factory.AdqmRestMppwKafkaRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.KafkaMppwRequestHandler;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.MppwStartRequestHandler;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.RestLoadClient;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.service.mock.MockStatusReporter;
import ru.datamart.prostore.query.execution.plugin.adqm.status.dto.StatusReportDto;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.function.Predicate;

import static ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType.KAFKA;
import static ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType.REST;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MppwStartRequestHandlerTest {
    private static final DdlProperties ddlProperties = new DdlProperties();

    private static final String TEST_TOPIC = "adqm_topic";
    private static final String TEST_CONSUMER_GROUP = "adqm_group";
    private final AdqmRestMppwKafkaRequestFactory mppwKafkaRequestFactory = mock(AdqmRestMppwKafkaRequestFactory.class);
    private final List<KafkaBrokerInfo> kafkaBrokers = Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092));

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster("test_arenadata");
    }

    @Test
    void testStartCallOrder() {
        Map<Predicate<String>, List<Map<String, Object>>> mockData = new HashMap<>();
        mockData.put(t -> t.contains("select engine_full"), Collections.singletonList(
                createRowMap("engine_full", "Distributed('test_arenadata', 'shares', 'accounts_actual_shard', column1)")
        ));
        mockData.put(t -> t.contains("select sorting_key"), Collections.singletonList(
                createRowMap("sorting_key", "column1, column2, sys_from")
        ));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.contains("CREATE TABLE IF NOT EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Nullable(Int64), column2 Nullable(Int64), column3 Nullable(String), column4 Nullable(Int64), sys_op Nullable(Int32)") &&
                        t.contains("ENGINE = Kafka()"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64,column2 Int64) ENGINE = MergeTree ORDER BY (column1,column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE = Distributed (test_arenadata, dev__shares, accounts_buffer_shard, cityHash64(column1))"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2 FROM dev__shares.accounts_ext_shard WHERE sys_op = 1"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT es.column1, es.column2, es.column3, es.column4, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as sys_close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard es WHERE es.sys_op <> 1")
        ), mockData, false);

        MockStatusReporter mockReporter = createMockReporter(TEST_CONSUMER_GROUP + "_dev__shares.accounts");
        RestLoadClient mockInitiator = Mockito.mock(RestLoadClient.class);
        KafkaMppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties,
                createMppwProperties(KAFKA),
                mockReporter, mockInitiator, mppwKafkaRequestFactory, new AdqmTablesSqlFactory(ddlProperties));

        MppwKafkaRequest request = getRequest();
        when(mppwKafkaRequestFactory.create(request)).thenReturn(getLoadRequest(request));

        handler.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
                    assertTrue(mockReporter.wasCalled("start"));
                    verify(mockInitiator, never()).initiateLoading(any());
                });
    }

    @Test
    void testStartCallOrderWithRest() {
        Map<Predicate<String>, List<Map<String, Object>>> mockData = new HashMap<>();
        mockData.put(t -> t.contains("select engine_full"),
                Collections.singletonList(
                        createRowMap("engine_full", "Distributed('test_arenadata', 'shares', 'accounts_actual_shard', column1)")
                ));
        mockData.put(t -> t.contains("select sorting_key"),
                Collections.singletonList(
                        createRowMap("sorting_key", "column1, column2, sys_from")
                ));

        DatabaseExecutor executor = new MockDatabaseExecutor(Arrays.asList(
                t -> t.contains("CREATE TABLE IF NOT EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata") &&
                        t.contains("column1 Int64, column2 Int64, column3 Nullable(String), column4 Nullable(Int64), sys_op Nullable(Int32)") &&
                        t.contains("ENGINE = MergeTree()") &&
                        t.contains("ORDER BY (column1, column2)"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_ext_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("DROP TABLE IF EXISTS dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer_shard ON CLUSTER test_arenadata (column1 Int64,column2 Int64) ENGINE = MergeTree ORDER BY (column1,column2)"),
                t -> t.equalsIgnoreCase("CREATE TABLE dev__shares.accounts_buffer ON CLUSTER test_arenadata AS dev__shares.accounts_buffer_shard ENGINE = Distributed (test_arenadata, dev__shares, accounts_buffer_shard, cityHash64(column1))"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_buffer_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_buffer\n" +
                        "  AS SELECT column1, column2 FROM dev__shares.accounts_ext_shard WHERE sys_op = 1"),
                t -> t.equalsIgnoreCase("CREATE MATERIALIZED VIEW IF NOT EXISTS dev__shares.accounts_actual_loader_shard ON CLUSTER test_arenadata TO dev__shares.accounts_actual\n" +
                        "AS SELECT es.column1, es.column2, es.column3, es.column4, 101 AS sys_from, 9223372036854775807 as sys_to, 0 as sys_op_load, '9999-12-31 00:00:00' as sys_close_date, 1 AS sign  FROM dev__shares.accounts_ext_shard es WHERE es.sys_op <> 1")
        ), mockData, false);

        MockStatusReporter mockReporter = createMockReporter("restConsumerGroup");
        RestLoadClient mockInitiator = Mockito.mock(RestLoadClient.class);
        when(mockInitiator.initiateLoading(any())).thenReturn(Future.succeededFuture());

        KafkaMppwRequestHandler handler = new MppwStartRequestHandler(executor, ddlProperties,
                createMppwProperties(REST),
                mockReporter, mockInitiator, mppwKafkaRequestFactory, new AdqmTablesSqlFactory(ddlProperties));

        MppwKafkaRequest request = getRequest();
        when(mppwKafkaRequestFactory.create(request)).thenReturn(getLoadRequest(request));

        handler.execute(request).onComplete(ar -> {
            assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().getMessage() : "");
            assertTrue(mockReporter.wasCalled("start"));
            verify(mockInitiator, only()).initiateLoading(any());
        });
    }

    private Map<String, Object> createRowMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private MockStatusReporter createMockReporter(String expectedConsumerGroup) {
        Map<String, StatusReportDto> expected = new HashMap<>();
        expected.put("start", new StatusReportDto(TEST_TOPIC, expectedConsumerGroup));
        return new MockStatusReporter(expected);
    }

    private String getSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"column4\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

    private AdqmMppwProperties createMppwProperties(LoadType loadType) {
        AdqmMppwProperties adqmMppwProperties = new AdqmMppwProperties();
        adqmMppwProperties.setConsumerGroup(TEST_CONSUMER_GROUP);
        adqmMppwProperties.setKafkaBrokers("localhost:9092");
        adqmMppwProperties.setLoadType(loadType);
        adqmMppwProperties.setRestLoadConsumerGroup("restConsumerGroup");
        return adqmMppwProperties;
    }

    private MppwKafkaRequest getRequest() {
        return MppwKafkaRequest.builder()
                .requestId(UUID.randomUUID())
                .datamartMnemonic("shares")
                .envName("dev")
                .loadStart(true)
                .sysCn(101L)
                .destinationEntity(getEntity())
                .topic(TEST_TOPIC)
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .externalSchema(getSchema())
                        .format(ExternalTableFormat.AVRO)
                        .uploadMessageLimit(1000)
                        .build())
                .brokers(kafkaBrokers)
                .build();
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
                                .build(),
                        EntityField.builder()
                                .name("column4")
                                .type(ColumnType.DATE)
                                .ordinalPosition(4)
                                .build()
                ))
                .build();
    }

    private RestMppwKafkaLoadRequest getLoadRequest(MppwKafkaRequest request) {
        return RestMppwKafkaLoadRequest.builder()
                .requestId(request.getRequestId().toString())
                .datamart(request.getDatamartMnemonic())
                .tableName(request.getDestinationEntity().getName())
                .kafkaTopic(request.getTopic())
                .kafkaBrokers(request.getBrokers())
                .hotDelta(request.getSysCn())
                .consumerGroup("mppwProperties.getRestLoadConsumerGroup()")
                .format(request.getUploadMetadata().getFormat().getName())
                .schema(new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema()))
                .messageProcessingLimit(Optional.ofNullable(((UploadExternalEntityMetadata) request.getUploadMetadata())
                        .getUploadMessageLimit())
                        .orElse(0))
                .build();
    }
}
