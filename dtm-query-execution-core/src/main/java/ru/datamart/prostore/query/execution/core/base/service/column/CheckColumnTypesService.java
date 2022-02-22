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
package ru.datamart.prostore.query.execution.core.base.service.column;

import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class CheckColumnTypesService {

    public static final String FAIL_CHECK_COLUMNS_PATTERN = "The types of columns of the destination table [%s] " +
            "and the types of the selection columns does not match!";

    public boolean check(List<EntityField> destinationFields, RelRoot relNode) {
        val destinationColumns = destinationFields.stream()
                .map(field -> CalciteUtil.valueOf(field.getType()))
                .collect(Collectors.toList());
        val sourceColumns =
                relNode.validatedRowType.getFieldList().stream()
                        .map(field -> field.getType().getSqlTypeName())
                        .collect(Collectors.toList());
        if (destinationColumns.size() != sourceColumns.size())
            return false;
        for (int i = 0; i < destinationColumns.size(); i++) {
            if (!equals(destinationColumns.get(i), sourceColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean equals(SqlTypeName destinationType, SqlTypeName sourceType) {
        if (destinationType.equals(sourceType)) {
            return true;
        }
        return destinationType.equals(SqlTypeName.INTEGER) && sourceType.equals(SqlTypeName.SMALLINT) ||
                destinationType.equals(SqlTypeName.SMALLINT) && sourceType.equals(SqlTypeName.INTEGER);
    }
}
