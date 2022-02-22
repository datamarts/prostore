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
package ru.datamart.prostore.query.execution.core.ddl.utils;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.visitors.SqlInvalidTimestampFinder;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import lombok.val;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class ValidationUtils {
    private static final String ALLOWED_NAME_PATTERN = "[a-zA-Z][a-zA-Z0-9_]*";
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile(ALLOWED_NAME_PATTERN);

    private ValidationUtils() {
    }

    public static void checkCharFieldsSize(List<EntityField> fields) {
        val notSetSizeFields = fields.stream()
                .filter(field -> !isValidSize(field))
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (!notSetSizeFields.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Specifying the size for columns%s with types[CHAR/VARCHAR] is required", notSetSizeFields)
            );
        }
    }

    private static boolean isValidSize(EntityField field) {
        val type = field.getType();
        val size = field.getSize();
        switch (type) {
            case CHAR:
                return size != null && size > 0;
            case VARCHAR:
                return size == null || size > 0;
            default:
                return true;
        }
    }

    public static void checkRequiredKeys(List<EntityField> fields) {
        val notExistsKeys = new ArrayList<String>();
        val notExistsPrimaryKeys = fields.stream()
                .noneMatch(f -> f.getPrimaryOrder() != null);
        if (notExistsPrimaryKeys) {
            notExistsKeys.add("primary key(s)");
        }

        val notExistsShardingKey = fields.stream()
                .noneMatch(f -> f.getShardingOrder() != null);
        if (notExistsShardingKey) {
            notExistsKeys.add("sharding key(s)");
        }

        if (!notExistsKeys.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Primary keys and Sharding keys are required. The following keys do not exist: %s",
                            String.join(",", notExistsKeys)));
        }
    }


    public static void checkFieldsDuplication(List<EntityField> fields) {
        Set<String> uniqueFieldNames = fields.stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());

        if (uniqueFieldNames.size() != fields.size()) {
            throw new ValidationDtmException("Entity has duplication fields names");
        }
    }

    public static void checkTimestampFormat(SqlNode node) {
        val finder = new SqlInvalidTimestampFinder();
        node.accept(finder);
        if (!finder.getInvalidTimestamps().isEmpty()) {
            throw new ValidationDtmException(String.format("Query contains invalid TIMESTAMP format [yyyy-MM-dd HH:mm:ss(.mmmmmm)]: %s", finder.getInvalidTimestamps()));
        }
    }

    public static void checkShardingKeys(List<EntityField> fields) {
        if (fields.stream()
                .anyMatch(field -> field.getPrimaryOrder() == null && field.getShardingOrder() != null)) {
            throw new ValidationDtmException("DISTRIBUTED BY clause must be a subset of the PRIMARY KEY");
        }

    }

    public static void checkEntityNames(Entity entity) {
        if (isNotValidName(entity.getSchema()) || isNotValidName(entity.getName())) {
            throw new ValidationDtmException(
                    String.format("Entity name [%s] is not valid, allowed pattern is %s",
                            entity.getNameWithSchema(), ALLOWED_NAME_PATTERN)
            );
        }

        if (entity.getFields() == null) {
            return;
        }

        val notValidFieldNames = entity.getFields().stream()
                .map(EntityField::getName)
                .filter(ValidationUtils::isNotValidName)
                .collect(Collectors.toList());
        if (!notValidFieldNames.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Entity columns %s name is not valid, allowed pattern is %s",
                            notValidFieldNames, ALLOWED_NAME_PATTERN)
            );
        }
    }

    public static void checkName(String name) {
        if (isNotValidName(name)) {
            throw new ValidationDtmException(
                    String.format("Identifier [%s] is not valid, allowed pattern is %s", name, ALLOWED_NAME_PATTERN)
            );
        }
    }

    private static boolean isNotValidName(String name) {
        return !VALID_NAME_PATTERN.matcher(name).matches();
    }
}
