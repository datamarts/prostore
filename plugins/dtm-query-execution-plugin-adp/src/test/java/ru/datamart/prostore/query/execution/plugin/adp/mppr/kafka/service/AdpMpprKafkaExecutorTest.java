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
package ru.datamart.prostore.query.execution.plugin.adp.mppr.kafka.service;

import io.vertx.core.Future;
import org.apache.avro.SchemaParseException;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMpprRequest;
import ru.datamart.prostore.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdpMpprKafkaExecutorTest {
    private static final String ENRICHED = "ENRICHED";
    public static final String ENV = "env";
    public static final String DATAMART = "datamart";
    public static final int CHUNK_SIZE = 1000;
    public static final String SCHEMA = "{\"type\":\"record\",\"name\":\"table\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    public static final String KAFKA_HOST = "localhost";
    public static final int KAFKA_PORT = 1234;
    public static final String TOPIC = "topic";
    public static final String SQL = "sql";

    @Mock
    private QueryParserResponse parserResponse;
    @Mock
    private QueryParserService queryParserService;
    @Mock
    private QueryEnrichmentService queryEnrichmentService;
    @Mock
    private AdpConnectorClient adpConnectorClient;
    @InjectMocks
    private AdpMpprKafkaExecutor adpMpprKafkaExecutor;


    @Captor
    private ArgumentCaptor<QueryParserRequest> queryParserRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<EnrichQueryRequest> enrichQueryRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<AdpConnectorMpprRequest> connectorMpprRequestArgumentCaptor;

    @Test
    void shouldSuccessWhenAllStepFinished() {
        // arrange
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(queryEnrichmentService.enrich(Mockito.any())).thenReturn(Future.succeededFuture(ENRICHED));
        when(adpConnectorClient.runMppr(Mockito.any())).thenReturn(Future.succeededFuture());

        UUID requestId = UUID.randomUUID();
        SqlNodeList sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList dmlSubQuery = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> logicalSchema = Arrays.asList(new Datamart());
        List<DeltaInformation> deltaInformations = Arrays.asList();
        MpprKafkaRequest request = new MpprKafkaRequest(requestId, ENV, DATAMART, sqlNode, logicalSchema, emptyList(),
                new Entity(), deltaInformations, dmlSubQuery,
                new DownloadExternalEntityMetadata("name", "path", ExternalTableFormat.AVRO, SCHEMA, CHUNK_SIZE),
                singletonList(new KafkaBrokerInfo(KAFKA_HOST, KAFKA_PORT)), TOPIC, SQL);

        // act
        Future<QueryResult> execute = adpMpprKafkaExecutor.execute(request);

        // assert
        verify(queryParserService).parse(queryParserRequestArgumentCaptor.capture());
        QueryParserRequest queryParserRequest = queryParserRequestArgumentCaptor.getValue();
        assertSame(dmlSubQuery, queryParserRequest.getQuery());
        assertSame(logicalSchema, queryParserRequest.getSchema());

        verify(queryEnrichmentService).enrich(enrichQueryRequestArgumentCaptor.capture());
        EnrichQueryRequest enrichQueryRequest = enrichQueryRequestArgumentCaptor.getValue();
        assertSame(deltaInformations, enrichQueryRequest.getDeltaInformations());
        assertSame(parserResponse.getCalciteContext(), enrichQueryRequest.getCalciteContext());
        assertSame(parserResponse.getRelNode(), enrichQueryRequest.getRelNode());
        assertEquals(ENV, enrichQueryRequest.getEnvName());

        verify(adpConnectorClient).runMppr(connectorMpprRequestArgumentCaptor.capture());
        AdpConnectorMpprRequest adpConnectorMpprRequest = connectorMpprRequestArgumentCaptor.getValue();
        assertEquals(requestId.toString(), adpConnectorMpprRequest.getRequestId());
        assertEquals(SQL, adpConnectorMpprRequest.getTable());
        assertEquals(DATAMART, adpConnectorMpprRequest.getDatamart());
        assertEquals(ENRICHED, adpConnectorMpprRequest.getSql());
        assertEquals(1, adpConnectorMpprRequest.getKafkaBrokers().size());
        assertEquals(KAFKA_HOST, adpConnectorMpprRequest.getKafkaBrokers().get(0).getHost());
        assertEquals(KAFKA_PORT, adpConnectorMpprRequest.getKafkaBrokers().get(0).getPort());
        assertEquals(TOPIC, adpConnectorMpprRequest.getKafkaTopic());
        assertEquals(CHUNK_SIZE, adpConnectorMpprRequest.getChunkSize());
        assertEquals(SCHEMA, adpConnectorMpprRequest.getAvroSchema().toString());

        if (execute.failed()) {
            fail(execute.cause());
        }
        assertTrue(execute.succeeded());
    }

    @Test
    void shouldFailWhenParsingFailed() {
        // arrange
        when(queryParserService.parse(any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        lenient().when(queryEnrichmentService.enrich(Mockito.any())).thenReturn(Future.succeededFuture(ENRICHED));
        lenient().when(adpConnectorClient.runMppr(Mockito.any())).thenReturn(Future.succeededFuture());

        UUID requestId = UUID.randomUUID();
        SqlNodeList sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList dmlSubQuery = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> logicalSchema = Arrays.asList(new Datamart());
        List<DeltaInformation> deltaInformations = Arrays.asList();
        MpprKafkaRequest request = new MpprKafkaRequest(requestId, ENV, DATAMART, sqlNode, logicalSchema, emptyList(),
                new Entity(), deltaInformations, dmlSubQuery,
                new DownloadExternalEntityMetadata("name", "path", ExternalTableFormat.AVRO, SCHEMA, CHUNK_SIZE),
                singletonList(new KafkaBrokerInfo(KAFKA_HOST, KAFKA_PORT)), TOPIC, SQL);

        // act
        Future<QueryResult> execute = adpMpprKafkaExecutor.execute(request);

        // assert
        verify(queryParserService).parse(Mockito.any());
        verifyNoInteractions(queryEnrichmentService);
        verifyNoInteractions(adpConnectorClient);

        if (execute.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(execute.failed());
        assertSame(RuntimeException.class, execute.cause().getClass());
    }

    @Test
    void shouldFailWhenEnrichmentFailed() {
        // arrange
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        lenient().when(queryEnrichmentService.enrich(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));
        lenient().when(adpConnectorClient.runMppr(Mockito.any())).thenReturn(Future.succeededFuture());

        UUID requestId = UUID.randomUUID();
        SqlNodeList sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList dmlSubQuery = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> logicalSchema = Arrays.asList(new Datamart());
        List<DeltaInformation> deltaInformations = Arrays.asList();
        MpprKafkaRequest request = new MpprKafkaRequest(requestId, ENV, DATAMART, sqlNode, logicalSchema, emptyList(),
                new Entity(), deltaInformations, dmlSubQuery,
                new DownloadExternalEntityMetadata("name", "path", ExternalTableFormat.AVRO, SCHEMA, CHUNK_SIZE),
                singletonList(new KafkaBrokerInfo(KAFKA_HOST, KAFKA_PORT)), TOPIC, SQL);

        // act
        Future<QueryResult> execute = adpMpprKafkaExecutor.execute(request);

        // assert
        verify(queryParserService).parse(Mockito.any());
        verify(queryEnrichmentService).enrich(any());
        verifyNoInteractions(adpConnectorClient);

        if (execute.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(execute.failed());
        assertSame(RuntimeException.class, execute.cause().getClass());
    }

    @Test
    void shouldFailWhenConnectorFailed() {
        // arrange
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(queryEnrichmentService.enrich(Mockito.any())).thenReturn(Future.succeededFuture(ENRICHED));
        when(adpConnectorClient.runMppr(Mockito.any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        UUID requestId = UUID.randomUUID();
        SqlNodeList sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList dmlSubQuery = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> logicalSchema = Arrays.asList(new Datamart());
        List<DeltaInformation> deltaInformations = Arrays.asList();
        MpprKafkaRequest request = new MpprKafkaRequest(requestId, ENV, DATAMART, sqlNode, logicalSchema, emptyList(),
                new Entity(), deltaInformations, dmlSubQuery,
                new DownloadExternalEntityMetadata("name", "path", ExternalTableFormat.AVRO, SCHEMA, CHUNK_SIZE),
                singletonList(new KafkaBrokerInfo(KAFKA_HOST, KAFKA_PORT)), TOPIC, SQL);

        // act
        Future<QueryResult> execute = adpMpprKafkaExecutor.execute(request);

        // assert
        verify(queryParserService).parse(Mockito.any());
        verify(queryEnrichmentService).enrich(any());
        verify(adpConnectorClient).runMppr(any());

        if (execute.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(execute.failed());
        assertSame(RuntimeException.class, execute.cause().getClass());
    }

    @Test
    void shouldFailWhenIncorrectSchema() {
        // arrange
        when(queryParserService.parse(any())).thenReturn(Future.succeededFuture(parserResponse));
        when(queryEnrichmentService.enrich(Mockito.any())).thenReturn(Future.succeededFuture(ENRICHED));

        UUID requestId = UUID.randomUUID();
        SqlNodeList sqlNode = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList dmlSubQuery = new SqlNodeList(SqlParserPos.ZERO);
        List<Datamart> logicalSchema = Arrays.asList(new Datamart());
        List<DeltaInformation> deltaInformations = Arrays.asList();
        MpprKafkaRequest request = new MpprKafkaRequest(requestId, ENV, DATAMART, sqlNode, logicalSchema, emptyList(),
                new Entity(), deltaInformations, dmlSubQuery,
                new DownloadExternalEntityMetadata("name", "path", ExternalTableFormat.AVRO, "WRONG", CHUNK_SIZE),
                singletonList(new KafkaBrokerInfo(KAFKA_HOST, KAFKA_PORT)), TOPIC, SQL);

        // act
        Future<QueryResult> execute = adpMpprKafkaExecutor.execute(request);

        // assert
        verify(queryParserService).parse(Mockito.any());
        verify(queryEnrichmentService).enrich(any());

        if (execute.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(execute.failed());
        assertSame(SchemaParseException.class, execute.cause().getClass());
    }
}