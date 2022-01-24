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
package io.arenadata.dtm.query.execution.core.query;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.base.dto.request.CoreRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.query.factory.QueryRequestFactory;
import io.arenadata.dtm.query.execution.core.query.factory.RequestContextFactory;
import io.arenadata.dtm.query.execution.core.query.service.QueryAnalyzer;
import io.arenadata.dtm.query.execution.core.query.service.QueryDispatcher;
import io.arenadata.dtm.query.execution.core.query.service.QuerySemicolonRemover;
import io.arenadata.dtm.query.execution.core.query.utils.DatamartMnemonicExtractor;
import io.arenadata.dtm.query.execution.core.query.utils.DefaultDatamartSetter;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class QueryAnalyzerTest {
    private final Vertx vertx = Vertx.vertx();
    private final RequestContextFactory requestContextFactory =
            new RequestContextFactory(TestUtils.getCoreConfiguration("test"));
    private final QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
    private QueryAnalyzer queryAnalyzer;

    @BeforeEach
    void setUp() {
        queryAnalyzer = new QueryAnalyzer(queryDispatcher,
                TestUtils.DEFINITION_SERVICE,
                requestContextFactory,
                vertx,
                new DatamartMnemonicExtractor(new DeltaInformationExtractor()),
                new DefaultDatamartSetter(),
                new QuerySemicolonRemover(),
                new QueryRequestFactory());
    }

    @Test
    void parsedSelect() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("select count(*) from (SelEct * from TEST_DATAMART.PSO where LST_NAM='test' " +
                "union all " +
                "SelEct * from TEST_DATAMART.PSO where LST_NAM='test1') " +
                "group by ID " +
                "order by 1 desc");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseInsertSelect() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("INSERT INTO TEST_DATAMART.PSO SELECT * FROM TEST_DATAMART.PSO");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.EDML, testData.getProcessingType());
    }

    @Test
    void parseSelectWithHintReturnsComplete() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO DATASOURCE_TYPE='ADB'");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals("test_datamart"
                , testData.getRequest().getDatamartMnemonic());
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseSelectForSystemTime() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("SELECT * FROM TEST_DATAMART.PSO for system_time" +
                " as of '2011-01-02 00:00:00' where 1=1");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DML, testData.getProcessingType());
    }

    @Test
    void parseEddl() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP DOWNLOAD EXTERNAL TABLE test.s");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(queryRequest.getSql(), testData.getRequest().getSql());
        assertEquals(SqlProcessingType.EDDL, testData.getProcessingType());
    }

    @Test
    void parseDdl() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("DROP TABLE r.l");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    @Test
    void parseGetDelta() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_BY_NUM(1)");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseGetDeltaOk() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_OK()");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseGetDeltaHot() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_HOT()");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseGetDeltaByDateTime() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("GET_DELTA_BY_DATETIME('2018-01-01')");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseBeginDelta() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("BEGIN DELTA");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseCommitDelta() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("COMMIT DELTA");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseRollbackDelta() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ROLLBACK DELTA");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DELTA, testData.getProcessingType());
    }

    @Test
    void parseCheckDatamart() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE(datamart)");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckDatamartWithoutDatamart() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE()");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckDatamartWithoutBraces() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATABASE");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckTable() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_TABLE(tbl1)");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckData() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_DATA(tbl1, 1)");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckSum() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_SUM(1)");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseCheckVersions() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CHECK_VERSIONS()");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CHECK, testData.getProcessingType());
    }

    @Test
    void parseConfigAddStorage() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("CONFIG_STORAGE_ADD('ADB') ");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.CONFIG, testData.getProcessingType());
    }

    @Test
    void parseAlterView() {
        InputQueryRequest queryRequest = new InputQueryRequest();
        queryRequest.setSql("ALTER VIEW test.view_a AS SELECT * FROM test.test_data");

        TestData testData = prepareExecute();
        analyzeAndExecute(testData, queryRequest);

        assertThat(testData.getResult()).isEqualToIgnoringCase("complete");
        assertEquals(SqlProcessingType.DDL, testData.getProcessingType());
    }

    private void analyzeAndExecute(TestData testData, InputQueryRequest queryRequest) {
        TestSuite suite = TestSuite.create("parse");
        suite.test("parse", context -> {
            Async async = context.async();
            queryAnalyzer.analyzeAndExecute(queryRequest)
                    .onComplete(res -> {
                        testData.setResult("complete");
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private TestData prepareExecute() {
        TestData testData = new TestData();
        doAnswer(invocation -> {
            final CoreRequestContext ddlRequest = invocation.getArgument(0);
            testData.setRequest(ddlRequest.getRequest().getQueryRequest());
            testData.setProcessingType(ddlRequest.getProcessingType());
            return Future.succeededFuture(QueryResult.emptyResult());
        }).when(queryDispatcher).dispatch(any());
        return testData;
    }

    @Data
    private static class TestData {

        private String result;
        private QueryRequest request;
        private SqlProcessingType processingType;

        String getResult() {
            return result;
        }

        void setResult(String result) {
            this.result = result;
        }
    }
}
