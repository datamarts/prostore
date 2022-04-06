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
package ru.datamart.prostore.query.execution.plugin.adp.util;

import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Assertions;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.service.AdpCalciteDefinitionService;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adp.base.utils.AdpTypeUtil.adpTypeFromDtmType;

public class TestUtils {

    public static final SqlParser.Config CONFIG_PARSER = SqlParser.configBuilder()
            .setParserFactory(new CalciteCoreConfiguration().eddlParserImplFactory())
            .setConformance(SqlConformanceEnum.DEFAULT)
            .setCaseSensitive(false)
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.TO_LOWER)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build();

    public static final DefinitionService<SqlNode> DEFINITION_SERVICE = new AdpCalciteDefinitionService(CONFIG_PARSER);
    public static final String SCHEMA = "datamart";
    public static final String TABLE = "table";
    public static final String LOCATION_PATH = "table_path";
    public static final String ENV = "env";

    public static DdlRequest createDdlRequest() {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, new Entity(), null);
    }

    public static DdlRequest createDdlRequest(Entity entity) {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, entity, null);
    }

    public static Entity createAllTypesTable() {
        List<ColumnType> allTypes = Arrays.stream(ColumnType.values())
                .filter(type -> !type.equals(ColumnType.BLOB) && !type.equals(ColumnType.ANY))
                .collect(Collectors.toList());
        List<ColumnType> sizedTimes = Arrays.asList(ColumnType.TIMESTAMP, ColumnType.TIME);
        List<ColumnType> sizedChars = Arrays.asList(ColumnType.VARCHAR, ColumnType.CHAR);
        List<EntityField> fields = new ArrayList<>();
        fields.add(createEntityField(0, "id", ColumnType.INT, null, false, 1, 1));
        for (int i = 0; i < allTypes.size(); i++) {
            val columnType = allTypes.get(i);
            Integer size = null;
            if (sizedTimes.contains(columnType)) {
                size = 6;
            }
            if (sizedChars.contains(columnType)) {
                size = 10;
            }
            if (columnType.equals(ColumnType.UUID)) {
                size = 36;
            }
            fields.add(createEntityField(
                    i + 1,
                    columnType.name().toLowerCase() + "_col",
                    columnType,
                    size));
        }
        return Entity.builder()
                .schema(SCHEMA)
                .name(TABLE)
                .externalTableLocationPath(LOCATION_PATH)
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
    }

    public static List<AdpTableColumn> adpTableColumnsFromEntityFields(List<EntityField> fields) {
        return fields.stream()
                .map(field -> new AdpTableColumn(field.getName(), adpTypeFromDtmType(field.getType(), field.getSize()), field.getNullable()))
                .collect(Collectors.toList());
    }

    public static List<AdpTableColumn> adpTableColumnsFromEntityFieldsWithNullable(List<EntityField> fields) {
        return fields.stream()
                .map(field -> new AdpTableColumn(field.getName(), adpTypeFromDtmType(field.getType(), field.getSize()), true))
                .collect(Collectors.toList());
    }

    private static EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size,
                                                 boolean nullable, Integer primaryOrder, Integer shardingOrder) {
        return EntityField.builder()
                .ordinalPosition(ordinalPosition)
                .name(name)
                .type(type)
                .size(size)
                .accuracy(null)
                .nullable(nullable)
                .primaryOrder(primaryOrder)
                .shardingOrder(shardingOrder)
                .defaultValue(null)
                .build();
    }

    private static EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size) {
        return createEntityField(ordinalPosition, name, type, size, true, null, null);
    }

    public static EddlRequest createEddlRequest(boolean isCreate) {
        return EddlRequest.builder()
                .createRequest(isCreate)
                .requestId(UUID.randomUUID())
                .envName(ENV)
                .entity(createAllTypesTable())
                .datamartMnemonic(SCHEMA)
                .build();
    }

    public static void assertNormalizedEquals(String actual, String expected) {
        if (actual == null || expected == null) {
            Assertions.assertEquals(expected, actual);
            return;
        }

        String fixedActual = actual.replaceAll("\r\n|\r|\n", " ");
        String fixedExpected = expected.replaceAll("\r\n|\r|\n", " ");
        Assertions.assertEquals(fixedExpected, fixedActual);
    }
}
