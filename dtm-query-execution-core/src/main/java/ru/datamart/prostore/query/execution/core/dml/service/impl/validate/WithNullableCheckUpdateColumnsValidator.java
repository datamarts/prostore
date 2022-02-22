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
package ru.datamart.prostore.query.execution.core.dml.service.impl.validate;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;

import java.util.stream.Collectors;

@Component
public class WithNullableCheckUpdateColumnsValidator implements UpdateColumnsValidator {

    private final BasicUpdateColumnsValidator basicUpdateColumnsValidator;

    public WithNullableCheckUpdateColumnsValidator(BasicUpdateColumnsValidator basicUpdateColumnsValidator) {
        this.basicUpdateColumnsValidator = basicUpdateColumnsValidator;
    }

    @Override
    public Future<Entity> validate(SqlNode sqlNode, Entity entity) {
        return basicUpdateColumnsValidator.validate(sqlNode, entity)
                .map(result -> {
                    val insertNode = (SqlInsert) sqlNode;
                    if (insertNode.getTargetColumnList() == null) {
                        return result;
                    }

                    val targetColumnNames = insertNode.getTargetColumnList().getList().stream()
                            .map(node -> ((SqlIdentifier) node).getSimple())
                            .collect(Collectors.toList());
                    val notNullableFields = EntityFieldUtils.getNotNullableFields(result).stream()
                            .map(EntityField::getName)
                            .collect(Collectors.toList());
                    if (!targetColumnNames.containsAll(notNullableFields)) {
                        throw new ValidationDtmException(String.format("NOT NULL constraint failed. Some non-nullable columns [%s] are not set",
                                String.join(", ", notNullableFields)));
                    }

                    return result;
                });
    }
}
