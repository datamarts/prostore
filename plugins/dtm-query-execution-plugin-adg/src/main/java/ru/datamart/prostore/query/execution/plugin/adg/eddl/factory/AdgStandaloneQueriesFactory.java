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
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.*;

import java.util.*;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.BUCKET_ID;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.ID;

@Service
public class AdgStandaloneQueriesFactory {
    private final SpaceEngines engine;

    public AdgStandaloneQueriesFactory(TarantoolDatabaseProperties tarantoolProperties) {
        this.engine = SpaceEngines.valueOf(tarantoolProperties.getEngine());
    }

    public OperationYaml create(Entity entity) {
        val yaml = new OperationYaml();
        val spacesMap = new LinkedHashMap<String, Space>();
        val spaceAttributes = createSpaceAttributes(entity);
        val shardingKeys = EntityFieldUtils.getShardingKeyNames(entity);
        val spaceIndexes = createIndexes(entity.getFields());
        val space = new Space(spaceAttributes, false, engine, false, shardingKeys, spaceIndexes);
        spacesMap.put(entity.getExternalTableLocationPath(), space);
        yaml.setSpaces(spacesMap);
        return yaml;
    }

    private List<SpaceAttribute> createSpaceAttributes(Entity entity) {
        return entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(this::createAttribute)
                .collect(Collectors.toList());
    }

    private List<SpaceIndex> createIndexes(List<EntityField> fields) {
        return Arrays.asList(
                new SpaceIndex(true, createPrimaryKeyParts(fields), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.singletonList(
                        new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)),
                        SpaceIndexTypes.TREE, BUCKET_ID)
        );
    }

    private List<SpaceIndexPart> createPrimaryKeyParts(List<EntityField> fields) {
        return EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(field -> new SpaceIndexPart(field.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(field.getType()).getName(), field.getNullable()))
                .collect(Collectors.toList());
    }

    private SpaceAttribute createAttribute(EntityField field) {
        val type = field.getName().equalsIgnoreCase(BUCKET_ID) ? SpaceAttributeTypes.UNSIGNED :
                SpaceAttributeTypeUtil.toAttributeType(field.getType());
        return new SpaceAttribute(field.getNullable(), field.getName(), type);
    }
}
