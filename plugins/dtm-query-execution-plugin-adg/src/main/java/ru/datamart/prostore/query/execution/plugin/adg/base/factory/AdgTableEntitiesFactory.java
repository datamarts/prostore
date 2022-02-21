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
package ru.datamart.prostore.query.execution.plugin.adg.base.factory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.dto.AdgTables;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.*;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;

import java.util.*;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;

@Service("adgTableEntitiesFactory")
public class AdgTableEntitiesFactory implements TableEntitiesFactory<AdgTables<Space>> {
    public static final String SEC_INDEX_PREFIX = "x_";
    private final SpaceEngines engine;

    @Autowired
    public AdgTableEntitiesFactory(TarantoolDatabaseProperties tarantoolProperties) {
        this.engine = SpaceEngines.valueOf(tarantoolProperties.getEngine());
    }

    @Override
    public AdgTables<Space> create(Entity entity, String env) {
        return new AdgTables<>(
                create(entity.getFields(), engine, ACTUAL_POSTFIX),
                create(entity.getFields(), engine, HISTORY_POSTFIX),
                create(entity.getFields(), engine, STAGING_POSTFIX)
        );
    }

    private Space create(List<EntityField> fields, SpaceEngines engine, String tablePostfix) {
        return new Space(
                createSpaceAttributes(fields, tablePostfix),
                false,
                engine,
                false,
                getShardingKey(fields),
                createSpaceIndexes(fields, tablePostfix));
    }

    private List<SpaceAttribute> createSpaceAttributes(List<EntityField> fields, String tablePosfix) {
        if (STAGING_POSTFIX.equalsIgnoreCase(tablePosfix)) {
                /* fields order:
                 1. logical fields
                 2. all system fields that are part of some index
                 3. bucket_id field
                */
            final SpaceAttribute bucketIdAttr = new SpaceAttribute(true, BUCKET_ID, SpaceAttributeTypes.UNSIGNED);
            final List<SpaceAttribute> indSysAttrs = createIndexedSysAttrs(tablePosfix);
            final List<SpaceAttribute> logicalFields = fields.stream()
                    .sorted(Comparator.comparing(EntityField::getOrdinalPosition))
                    .map(field -> createAttribute(field, field.getPrimaryOrder() == null))
                    .collect(Collectors.toList());
            List<SpaceAttribute> attributes = new ArrayList<>();
            attributes.addAll(logicalFields);
            attributes.addAll(indSysAttrs);
            attributes.add(bucketIdAttr);
            return attributes;
        } else {
                /* fields order:
                 1. fields from a primary key
                 2. bucket_id field
                 3. all system fields that are part of some index
                 4. logical fields
                */
            final List<SpaceAttribute> pkAttrs = EntityFieldUtils.getPrimaryKeyList(fields).stream()
                    .map(this::createAttribute)
                    .collect(Collectors.toList());
            final SpaceAttribute bucketIdAttr = new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED);
            final List<SpaceAttribute> indSysAttrs = createIndexedSysAttrs(tablePosfix);
            final List<SpaceAttribute> logicalNonPkAttrs = fields.stream()
                    .filter(f -> f.getPrimaryOrder() == null)
                    .map(this::createAttribute)
                    .collect(Collectors.toList());
            List<SpaceAttribute> attributes = new ArrayList<>(pkAttrs);
            attributes.add(bucketIdAttr);
            attributes.addAll(indSysAttrs);
            attributes.addAll(logicalNonPkAttrs);
            return attributes;
        }
    }

    private List<SpaceIndex> createSpaceIndexes(List<EntityField> fields, String tablePosfix) {
        switch (tablePosfix) {
            case ACTUAL_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, createPrimaryKeyPartsWithSysFrom(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            case HISTORY_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, createPrimaryKeyPartsWithSysFrom(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                        new SpaceIndex(false, Arrays.asList(
                                new SpaceIndexPart(SYS_TO_FIELD, SpaceAttributeTypes.NUMBER.getName(), true),
                                new SpaceIndexPart(SYS_OP_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_TO_FIELD),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            case STAGING_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, createPrimaryKeyParts(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), true)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            default:
                throw new DataSourceException(String.format("Table type [%s] doesn't support", tablePosfix));
        }
    }

    private List<SpaceAttribute> createIndexedSysAttrs(String tablePostfix) {
        switch (tablePostfix) {
            case ACTUAL_POSTFIX:
            case HISTORY_POSTFIX:
                return createSysAttrs();
            case STAGING_POSTFIX:
                return Collections.singletonList(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
            default:
                throw new DataSourceException(String.format("Unknown table prefix [%s]", tablePostfix));
        }
    }

    private List<SpaceAttribute> createSysAttrs() {
        return Arrays.asList(
                new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER),
                new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER),
                new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
    }

    private List<SpaceIndexPart> createPrimaryKeyPartsWithSysFrom(List<EntityField> fields) {
        final List<SpaceIndexPart> spaceIndexParts = EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
        spaceIndexParts.add(new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false));
        return spaceIndexParts;
    }

    private List<SpaceIndexPart> createPrimaryKeyParts(List<EntityField> fields) {
        return EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
    }

    private List<String> getShardingKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getShardingKeyList(fields).stream().map(EntityField::getName).collect(Collectors.toList());
        if (sk.isEmpty()) {
            sk = getPrimaryKey(fields);
        }
        return sk;
    }

    private List<String> getPrimaryKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getPrimaryKeyList(fields).stream().map(EntityField::getName).collect(Collectors.toList());
        if (sk.isEmpty()) {
            sk = Collections.singletonList(ID);
        }
        return sk;
    }

    private SpaceAttribute createAttribute(EntityField field) {
        return createAttribute(field, field.getNullable());
    }

    private SpaceAttribute createAttribute(EntityField field, boolean isNullable) {
        return new SpaceAttribute(isNullable, field.getName(), SpaceAttributeTypeUtil.toAttributeType(field.getType()));
    }
}
