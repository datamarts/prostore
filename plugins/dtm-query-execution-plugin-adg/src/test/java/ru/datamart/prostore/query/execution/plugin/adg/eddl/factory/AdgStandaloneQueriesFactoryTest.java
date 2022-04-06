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
package ru.datamart.prostore.query.execution.plugin.adg.eddl.factory;

import lombok.val;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.*;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.BUCKET_ID;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.ID;
import static ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils.createEntityWithBucketField;

class AdgStandaloneQueriesFactoryTest {
    private final TarantoolDatabaseProperties tarantoolProperties = new TarantoolDatabaseProperties();
    private final AdgStandaloneQueriesFactory factory = new AdgStandaloneQueriesFactory(tarantoolProperties);

    @Test
    void shouldCreateYaml() {
        val entity = createEntityWithBucketField();
        val expectedYaml = getExpectedYaml(entity);
        val actualYaml = factory.create(entity);
        assertThat(actualYaml).usingRecursiveComparison().isEqualTo(expectedYaml);
    }

    private OperationYaml getExpectedYaml(Entity entity) {
        OperationYaml expected = new OperationYaml();
        val expMap = new LinkedHashMap<String, Space>();
        val space = new Space(createSpaceAttributes(),
                false,
                SpaceEngines.valueOf(tarantoolProperties.getEngine()),
                false,
                EntityFieldUtils.getShardingKeyNames(entity),
                getTestIndexes());
        expMap.put(entity.getExternalTableLocationPath(), space);
        expected.setSpaces(expMap);
        return expected;
    }

    private List<SpaceAttribute> createSpaceAttributes() {
        val attributes = new ArrayList<SpaceAttribute>();
        attributes.addAll(createPkAttributes());
        attributes.addAll(createNonPkAttributes());
        return attributes;
    }

    private List<SpaceAttribute> createPkAttributes() {
         return Arrays.asList(
                new SpaceAttribute(false, "ID", SpaceAttributeTypes.INTEGER),
                new SpaceAttribute(false, "DESCRIPTION", SpaceAttributeTypes.STRING)
        );
    }

    private List<SpaceAttribute> createNonPkAttributes() {
        return Arrays.asList(
                new SpaceAttribute(true, "TEXT", SpaceAttributeTypes.STRING),
                new SpaceAttribute(false, "BUCKET_ID", SpaceAttributeTypes.UNSIGNED)
        );
    }

    private List<SpaceIndex> getTestIndexes() {
        return Arrays.asList(
                new SpaceIndex(true, createPrimaryKeyParts(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.singletonList(
                        new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)),
                        SpaceIndexTypes.TREE, BUCKET_ID)
        );
    }

    private List<SpaceIndexPart> createPrimaryKeyParts() {
        return Arrays.asList(
                new SpaceIndexPart("ID", SpaceAttributeTypeUtil.toAttributeType(ColumnType.INT).getName(), false),
                new SpaceIndexPart("DESCRIPTION", SpaceAttributeTypeUtil.toAttributeType(ColumnType.VARCHAR).getName(), false));
    }
}