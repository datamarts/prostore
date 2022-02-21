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
package ru.datamart.prostore.query.execution.plugin.adg.utils;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Assertions;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.CalciteDefinitionService;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.*;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;

import java.util.*;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgTableEntitiesFactory.SEC_INDEX_PREFIX;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;

public class TestUtils {
    public static final AdgCalciteConfiguration CALCITE_CONFIGURATION = new AdgCalciteConfiguration();
    public static final DefinitionService<SqlNode> DEFINITION_SERVICE =
            new CalciteDefinitionService(CALCITE_CONFIGURATION.configDdlParser(CALCITE_CONFIGURATION.ddlParserImplFactory())) {
            };

    public static final List<String> SPACE_POSTFIXES = Arrays.asList(ACTUAL_POSTFIX, HISTORY_POSTFIX, STAGING_POSTFIX);

    public static Entity getEntity() {
        List<EntityField> keyFields = Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "sk_key2", ColumnType.INT.name(), false, null, 2, null),
                new EntityField(2, "pk2", ColumnType.INT.name(), false, 2, null, null),
                new EntityField(3, "sk_key3", ColumnType.INT.name(), false, null, 3, null)
        );
        ColumnType[] types = ColumnType.values();
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            ColumnType type = types[i];
            if (Arrays.asList(ColumnType.BLOB, ColumnType.ANY).contains(type)) {
                continue;
            }

            EntityField.EntityFieldBuilder builder = EntityField.builder()
                    .ordinalPosition(i + keyFields.size())
                    .type(type)
                    .nullable(true)
                    .name(type.name() + "_type");
            if (Arrays.asList(ColumnType.CHAR, ColumnType.VARCHAR).contains(type)) {
                builder.size(20);
            } else if (Arrays.asList(ColumnType.TIME, ColumnType.TIMESTAMP).contains(type)) {
                builder.accuracy(5);
            }
            fields.add(builder.build());
        }
        fields.addAll(keyFields);
        return new Entity("test_schema.test_table", fields);
    }

    public static Map<String, Space> getSpaces(Entity entity) {
        Map<String, List<SpaceIndex>> spaceIndexMap = getSpaceIndexMap();
        List<SpaceAttribute> pkAttrs = EntityFieldUtils.getPrimaryKeyList(entity.getFields()).stream()
                .map(TestUtils::createAttribute)
                .collect(Collectors.toList());
        List<SpaceAttribute> logAttrs = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() == null)
                .map(TestUtils::createAttribute)
                .collect(Collectors.toList());
        List<SpaceAttribute> stageLogAttrs = entity.getFields().stream()
                .sorted(Comparator.comparing(EntityField::getOrdinalPosition))
                .map(field -> createAttribute(field, field.getPrimaryOrder() == null))
                .collect(Collectors.toList());
        return SPACE_POSTFIXES.stream()
                .collect(Collectors.toMap(
                        postfix -> String.format("env__%s__%s%s", entity.getSchema(), entity.getName(), postfix),
                        postfix -> Space.builder()
                                .format(postfix.equalsIgnoreCase(STAGING_POSTFIX)? getAttrs(postfix, Collections.emptyList() ,stageLogAttrs): getAttrs(postfix, pkAttrs, logAttrs))
                                .indexes(spaceIndexMap.get(postfix))
                                .build()));
    }

    public static Map<String, List<SpaceIndex>> getSpaceIndexMap() {
        Map<String, List<SpaceIndex>> spaceIndexMap = new HashMap<>();
        spaceIndexMap.put(ACTUAL_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(HISTORY_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_TO_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(STAGING_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        return spaceIndexMap;
    }

    public static List<SpaceAttribute> getAttrs(String postfix,
                                                List<SpaceAttribute> pkAttrs,
                                                List<SpaceAttribute> logAttrs) {
        List<SpaceAttribute> result;
        switch (postfix) {
            case ACTUAL_POSTFIX:
            case HISTORY_POSTFIX:
                result = new ArrayList<>(pkAttrs);
                result.add(new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED));
                result.add(new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
                result.addAll(logAttrs);
                break;
            default:
                result = new ArrayList<>(logAttrs);
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(true, BUCKET_ID, SpaceAttributeTypes.UNSIGNED));
                break;
        }
        return result;
    }

    private static SpaceAttribute createAttribute(EntityField field) {
        return createAttribute(field, false);
    }

    private static SpaceAttribute createAttribute(EntityField field, boolean overrideNullable) {
        return new SpaceAttribute(overrideNullable || field.getNullable(), field.getName(),
                SpaceAttributeTypeUtil.toAttributeType(field.getType()));
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
