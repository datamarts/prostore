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
package io.arenadata.dtm.query.execution.core.ddl.utils;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.visitors.SqlInvalidTimestampFinder;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import lombok.val;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class ValidationUtils {

    private ValidationUtils() {
    }

    public static void checkCharFieldsSize(List<EntityField> fields) {
        List<String> notSetSizeFields = fields.stream()
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
}
