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
package ru.datamart.prostore.common.model.ddl;

import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class EntityFieldUtils {

    private static final List<String> pkSystemField = Arrays.asList("sys_from");

    private EntityFieldUtils() {
    }

    public static List<EntityField> getPrimaryKeyList(final List<EntityField> fields) {
        return fields.stream()
                .filter(f -> f.getPrimaryOrder() != null)
                .sorted(Comparator.comparing(EntityField::getPrimaryOrder))
                .collect(toList());
    }

    public static List<EntityField> getPrimaryKeyListWithSysFields(final List<EntityField> fields) {
        return fields.stream()
                .filter(f -> f.getPrimaryOrder() != null || isSystemFieldForPk(f.getName()))
                .sorted(Comparator.comparing(EntityField::getPrimaryOrder))
                .collect(toList());
    }

    public static List<EntityField> getShardingKeyList(final List<EntityField> fields) {
        return fields.stream()
                .filter(f -> f.getShardingOrder() != null)
                .sorted(Comparator.comparing(EntityField::getShardingOrder))
                .collect(toList());
    }

    public static List<String> getShardingKeyNames(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyList();
        }

        return entity.getFields().stream()
                .filter(f -> f.getShardingOrder() != null)
                .sorted(Comparator.comparing(EntityField::getShardingOrder))
                .map(EntityField::getName)
                .collect(toList());
    }

    private static boolean isSystemFieldForPk(final String fieldName) {
        return pkSystemField.contains(fieldName);
    }

    public static List<String> getPkFieldNames(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyList();
        }

        return entity.getFields().stream()
                .filter(entityField -> entityField.getPrimaryOrder() != null)
                .sorted(Comparator.comparingInt(EntityField::getPrimaryOrder))
                .map(EntityField::getName)
                .collect(Collectors.toList());
    }

    public static List<EntityField> getNotNullableFields(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyList();
        }

        return entity.getFields().stream()
                .filter(entityField -> entityField.getPrimaryOrder() != null || !entityField.getNullable())
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .collect(Collectors.toList());
    }

    public static List<String> getFieldNames(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyList();
        }

        return entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getName)
                .collect(Collectors.toList());
    }

    public static Map<String, EntityField> getFieldsMap(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyMap();
        }

        return entity.getFields().stream()
                .collect(Collectors.toMap(EntityField::getName, entityField -> entityField));
    }

    public static List<ColumnType> getFieldTypes(Entity entity) {
        if (entity == null || CollectionUtils.isEmpty(entity.getFields())) {
            return Collections.emptyList();
        }
        return entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getType)
                .collect(toList());
    }
}
