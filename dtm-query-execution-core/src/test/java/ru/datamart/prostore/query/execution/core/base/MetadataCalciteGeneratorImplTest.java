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
package ru.datamart.prostore.query.execution.core.base;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataCalciteGeneratorImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private MetadataCalciteGenerator metadataCalciteGenerator;
    private Entity table;
    private Entity table2;

    @BeforeEach
    void setUp() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        metadataCalciteGenerator = new MetadataCalciteGenerator();
        final List<EntityField> fields = createFieldsForUplTable();
        final List<EntityField> fields2 = createFieldsForTable();
        table = new Entity("uplexttab", null, fields);
        table2 = new Entity("accounts", "shares", fields2);
    }

    private List<EntityField> createFieldsForTable() {
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        f1.setPrimaryOrder(1);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true );
        f2.setSize(100);
        EntityField f3 = new EntityField(2, "account_id", ColumnType.INT, false );
        f1.setPrimaryOrder(1);
        f3.setPrimaryOrder(2);
        f3.setShardingOrder(1);
        return new ArrayList<>(Arrays.asList(f1, f2, f3));
    }

    private List<EntityField> createFieldsForUplTable() {
        EntityField f1 = new EntityField(0,"id", ColumnType.INT, false );
        f1.setPrimaryOrder(1);
        EntityField f2 = new EntityField(1,"name", ColumnType.VARCHAR, true );
        f2.setSize(100);
        EntityField f3 = new EntityField(2,"booleanvalue", ColumnType.BOOLEAN, true );
        EntityField f4 = new EntityField(3,"charvalue", ColumnType.CHAR, true);
        EntityField f5 = new EntityField(4,"bgintvalue", ColumnType.BIGINT, true);
        EntityField f6 = new EntityField(5,"dbvalue", ColumnType.DOUBLE, true);
        EntityField f7 = new EntityField(6,"flvalue", ColumnType.FLOAT, true);
        EntityField f8 = new EntityField(7,"datevalue", ColumnType.DATE, true);
        EntityField f9 = new EntityField(8,"timevalue", ColumnType.TIME, true);
        f9.setAccuracy(6);
        EntityField f11 = new EntityField(9, "tsvalue", ColumnType.TIMESTAMP, true);
        f11.setAccuracy(10);
        return new ArrayList<>(Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9, f11));
    }

    @Test
    void generateTableMetadataWithoutSchema() throws SqlParseException {
        String sql = "CREATE UPLOAD EXTERNAL TABLE uplExtTab (" +
                "id integer not null," +
                " name varchar(100)," +
                " booleanValue boolean, " +
                " charValue char, " +
                " bgIntValue bigint, " +
                " dbValue double, " +
                " flValue float, " +
                " dateValue date, " +
                " timeValue time, " +
                " tsValue timestamp(10), " +
                " primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'";
        SqlNode sqlNode = planner.parse(sql);
        Entity entity = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table, entity);
    }

    @Test
    void shouldFailWhenUnknownPrimaryKey() throws SqlParseException {
        // arrange
        String sql = "CREATE TABLE pva_test.test\n" +
                "(\n" +
                "id int not null,\n" +
                "primary key (nonexistent_column)\n" +
                ")\n" +
                "distributed by (id)";
        SqlNode sqlNode = planner.parse(sql);

        // act assert
        assertThrows(DtmException.class, () -> metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode));
    }

    @Test
    void shouldFailWhenUnknownDistributedKey() throws SqlParseException {
        // arrange
        String sql = "CREATE TABLE pva_test.test\n" +
                "(\n" +
                "id int not null,\n" +
                "primary key (id)\n" +
                ")\n" +
                "distributed by (nonexistent_column)";
        SqlNode sqlNode = planner.parse(sql);

        // act assert
        assertThrows(DtmException.class, () -> metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode));
    }

    @Test
    void shouldFailWhenUnknownType() throws SqlParseException {
        // arrange
        String sql = "CREATE TABLE pva_test.test\n" +
                "(\n" +
                "id id_col not null,\n" +
                "primary key (id)\n" +
                ")\n" +
                "distributed by (id)";
        SqlNode sqlNode = planner.parse(sql);

        // act assert
        assertThrows(DtmException.class, () -> metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode));
    }

    @Test
    void shouldFailWhenUnknownTypeOnMaterializedView() throws SqlParseException {
        // arrange
        String sql = "CREATE MATERIALIZED VIEW pva_test.crash_test\n" +
                "(\n" +
                "  id id_col not null,\n" +
                "  primary key (id)\n" +
                ")\n" +
                "DISTRIBUTED BY (id)\n" +
                "DATASOURCE_TYPE (ADG)\n" +
                "AS\n" +
                "SELECT * FROM test\n" +
                "DATASOURCE_TYPE='adb'";
        SqlNode sqlNode = planner.parse(sql);

        // act assert
        assertThrows(DtmException.class, () -> metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode));
    }

    @Test
    void generateTableMetadataWithSchema() throws SqlParseException {
        String sql = "CREATE UPLOAD EXTERNAL TABLE test_datamart.uplExtTab (" +
                "id integer not null," +
                " name varchar(100)," +
                " booleanValue boolean, " +
                " charValue char, " +
                " bgIntValue bigint, " +
                " dbValue double, " +
                " flValue float, " +
                " dateValue date, " +
                " timeValue time, " +
                " tsValue timestamp(10), " +
                " primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'";
        SqlNode sqlNode = planner.parse(sql);
        table.setSchema("test_datamart");
        Entity entity = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table, entity);
    }

    @Test
    void generateTableMetadata() throws SqlParseException {
        String sql = "create table shares.accounts (id integer not null, name varchar(100)," +
                " account_id integer not null, primary key(id, account_id)) distributed by (account_id)";
        SqlNode sqlNode = planner.parse(sql);
        Entity entity = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table2, entity);
    }

}
