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
package ru.datamart.prostore.query.execution.core.utils;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.delta.DeltaInformationResult;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import ru.datamart.prostore.query.execution.core.query.utils.DefaultDatamartSetter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class DefaultDatamartSetterTest {
    private static final SqlDialect DIALECT = new SqlDialect(CalciteSqlDialect.EMPTY_CONTEXT);
    private static final String EXPECTED_SCHEMA = "demo";
    private final CalciteConfiguration config = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));
    private final DefaultDatamartSetter datamartSetter = new DefaultDatamartSetter();
    private final DeltaInformationExtractor deltaInformationExtractor =
            new DeltaInformationExtractor();

    @Test
    void setToSelect() {
        val sql = "select *, CASE WHEN (account_type = 'D' AND  amount >= 0) OR (account_type = 'C' AND  amount <= 0) THEN 'OK' ELSE 'NOT OK' END\n" +
                "  from (\n" +
                "    select a.account_id, coalesce(sum(amount),0) amount, account_type\n" +
                "    from shares.accounts a\n" +
                "    left join shares.transactions FOR SYSTEM_TIME AS OF '2020-06-30 16:18:58' t using(account_id)\n" +
                "    left join transactions2 t2 using(account_id)\n" +
                "    left join transactions3 using(account_id)\n" +
                "   group by a.account_id, account_type\n" +
                ")x";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(2L, countByExpectedSchema);
    }

    @Test
    void setToSelectWithBetween() {
        val sql = "select * from accounts FOR SYSTEM_TIME AS OF '2020-06-30 16:18:58'";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(1L, countByExpectedSchema);
    }

    @Test
    void setToInsert() {
        val sql = "INSERT INTO PSO1 SELECT * FROM PSO2 P2 WHERE EXISTS (SELECT * FROM tbl2 t2 WHERE t2.id = P2.id)";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(3L, countByExpectedSchema);
    }

    @Test
    void setToUpdate() {
        val sql = "UPDATE tbl1 t1 SET A=1, B=2, C=3 WHERE EXISTS (SELECT * FROM tbl2 t2 WHERE t1.id = t2.id)";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(2L, countByExpectedSchema);
    }

    @Test
    void setToCREATE_VIEW() {
        val sql = "CREATE VIEW view1 as select * from test2.tbl1 JOIN view2 FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(2L, countByExpectedSchema);
    }

    @Test
    void setToDROP_VIEW() {
        val sql = "DROP VIEW view1";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(1L, countByExpectedSchema);
    }

    @Test
    void setToDROP_TABLE() {
        val sql = "DROP TABLE tbl1";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        log.info(withDatamart);
        assertEquals(1L, countByExpectedSchema);
    }

    @Test
    void setToBEGIN_DELTA() {
        val sql = "BEGIN DELTA";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        log.info(withDatamart);
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        assertEquals(0L, countByExpectedSchema);
    }

    @Test
    void setToCOMMIT_DELTA() {
        val sql = "COMMIT DELTA";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        log.info(withDatamart);
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        assertEquals(0L, countByExpectedSchema);
    }

    @Test
    void setToCREATE_UPLOAD_EXT_TBL() {
        val sql = "CREATE UPLOAD EXTERNAL TABLE accounts_ext (account_id bigint, account_type varchar(1)) LOCATION 'kafka://10.92.6.44:9092/accounts1' FORMAT 'AVRO'";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        log.info(withDatamart);
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        assertEquals(1L, countByExpectedSchema);
    }

    @Test
    void setToCREATE_TABLE() {
        val sql = "CREATE TABLE table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) )";
        String withDatamart = datamartSetter.set(definitionService.processingQuery(sql), "demo").toSqlString(DIALECT).getSql();
        log.info(withDatamart);
        DeltaInformationResult result = deltaInformationExtractor.extract(definitionService.processingQuery(withDatamart));
        List<DeltaInformation> deltaInformations = result.getDeltaInformations();
        long countByExpectedSchema = deltaInformations.stream()
                .filter(d -> d.getSchemaName().equals(EXPECTED_SCHEMA))
                .count();
        assertEquals(1L, countByExpectedSchema);
    }
}
