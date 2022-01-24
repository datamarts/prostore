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
package io.arenadata.dtm.query.execution.core.base.service.metadata;

import com.google.common.collect.Lists;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.service.metadata.query.CoreDdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoreDdlQueryGeneratorTest {
    private final static String SCHEMA = "TEST";
    private final static String ENTITY_NAME_TABLE = "TEST_TABLE";
    private final static String ENTITY_NAME_VIEW = "TEST_VIEW";
    private final static String ENTITY_NAME_MATVIEW = "TEST_MATVIEW";

    private final static String CREATE_TABLE_SCRIPT_EXPECTED =
            "CREATE TABLE " + SCHEMA + "." + ENTITY_NAME_TABLE + " (" +
                    "ID INT NOT NULL, " +
                    "DESCRIPTION VARCHAR(200) NOT NULL, " +
                    "FOREIGN_KEY INT NOT NULL, " +
                    "TIME_COL TIME(5) NULL, " +
                    "PRIMARY KEY (ID)" +
                    ") DISTRIBUTED BY (ID) " +
                    "DATASOURCE_TYPE (ADB, ADQM)";

    private final static String CREATE_VIEW_SCRIPT_EXPECTED =
            "CREATE VIEW " + SCHEMA + "." + ENTITY_NAME_VIEW +
                    " AS SELECT TEST_TABLE2.NAME, " +
                    "TEST_TABLE.DESCRIPTION " +
                    "FROM TEST.TEST_TABLE " +
                    "LEFT JOIN TEST.TEST_TABLE2 ON TEST_TABLE2.ID = TEST_TABLE.FOREIGN_KEY " +
                    "ORDER BY TEST_TABLE2.NAME";

    private final static String CREATE_MATERIALIZED_VIEW_SCRIPT_EXPECTED =
            "CREATE MATERIALIZED VIEW " + SCHEMA + "." + ENTITY_NAME_MATVIEW +
                    " (ID INT NOT NULL, " +
                    "DESCRIPTION VARCHAR(200) NOT NULL, " +
                    "FOREIGN_KEY INT NOT NULL, " +
                    "COUNT INT NOT NULL, " +
                    "PRIMARY KEY (ID, FOREIGN_KEY)" +
                    ") DISTRIBUTED BY (ID, FOREIGN_KEY) " +
                    "DATASOURCE_TYPE (ADB, ADQM) " +
                    "AS SELECT ID AS ID, " +
                    "DESCRIPTION AS DESCRIPTION, " +
                    "FOREIGN_KEY AS FOREIGN_KEY, " +
                    "COUNT(*) AS COUNT " +
                    "FROM TEST.TEST_TABLE " +
                    "GROUP BY ID, DESCRIPTION, FOREIGN_KEY " +
                    "DATASOURCE_TYPE = 'ADQM'";
    private final DdlQueryGenerator ddlQueryGenerator = new CoreDdlQueryGenerator();
    private final static Map<String, List<EntityField>> ENTITY_FIELDS = new HashMap<>();

    @BeforeAll
    public static void setUp() {
        val idCol = EntityField.builder()
                .ordinalPosition(0)
                .name("ID")
                .type(ColumnType.INT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val descriptionCol = EntityField.builder()
                .ordinalPosition(1)
                .name("DESCRIPTION")
                .type(ColumnType.VARCHAR)
                .size(200)
                .nullable(false)
                .build();
        val foreignKeyCol = EntityField.builder()
                .ordinalPosition(2)
                .name("FOREIGN_KEY")
                .type(ColumnType.INT)
                .nullable(false)
                .build();
        val timeCol = EntityField.builder()
                .ordinalPosition(3)
                .name("TIME_COL")
                .type(ColumnType.TIME)
                .nullable(true)
                .accuracy(5)
                .build();
        val foreignKeyMatViewCol = EntityField.builder()
                .ordinalPosition(2)
                .name("FOREIGN_KEY")
                .type(ColumnType.INT)
                .primaryOrder(2)
                .shardingOrder(2)
                .nullable(false)
                .build();
        val count = EntityField.builder()
                .ordinalPosition(4)
                .name("COUNT")
                .type(ColumnType.INT)
                .nullable(false)
                .build();
        List<EntityField> tableFields = Lists.newArrayList(idCol, descriptionCol, foreignKeyCol, timeCol);
        ENTITY_FIELDS.put(ENTITY_NAME_TABLE, tableFields);
        List<EntityField> matViewFields = Lists.newArrayList(idCol, descriptionCol, foreignKeyMatViewCol, count);
        ENTITY_FIELDS.put(ENTITY_NAME_MATVIEW, matViewFields);
    }

    @Test
    void createTableScript() {
        Entity entity = Entity.builder()
                .name(ENTITY_NAME_TABLE)
                .entityType(EntityType.TABLE)
                .schema(SCHEMA)
                .fields(ENTITY_FIELDS.get(ENTITY_NAME_TABLE))
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADQM))
                .build();
        entity.setEntityType(EntityType.TABLE);
        assertEquals(CREATE_TABLE_SCRIPT_EXPECTED, ddlQueryGenerator.generateCreateTableQuery(entity));
    }

    @Test
    void createViewScript() {
        val queryViewAs = "select test_table2.name, " +
                "TEST_TABLE.description " +
                "from test.test_table " +
                "left JOIN test.test_table2 on test_table2.id = test_table.foreign_key " +
                "order by test_table2.name";
        Entity entity = Entity.builder()
                .name(ENTITY_NAME_VIEW)
                .entityType(EntityType.VIEW)
                .schema(SCHEMA)
                .viewQuery(queryViewAs)
                .build();
        assertEquals(CREATE_VIEW_SCRIPT_EXPECTED, ddlQueryGenerator.generateCreateViewQuery(entity, ""));
    }

    @Test
    void createMaterializedViewScript() {
        val queryViewAs = "select id as id, " +
                "description as description, " +
                "foreign_key as foreign_key, " +
                "count(*) as count " +
                "from test.test_table " +
                "group by id, description, foreign_key";
        Entity entity = Entity.builder()
                .name(ENTITY_NAME_MATVIEW)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .schema(SCHEMA)
                .fields(ENTITY_FIELDS.get(ENTITY_NAME_MATVIEW))
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADQM))
                .viewQuery(queryViewAs)
                .materializedDataSource(SourceType.ADQM)
                .build();
        assertEquals(CREATE_MATERIALIZED_VIEW_SCRIPT_EXPECTED,
                ddlQueryGenerator.generateCreateMaterializedView(entity));
    }

}