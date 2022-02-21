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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.util;

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.reader.QueryParameters;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimestampString;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.*;

public final class AdqmDmlUtils {
    public static final String TEMP_TABLE = "__a";
    public static final SqlNumericLiteral MAX_CN_LITERAL = SqlLiteral.createExactNumeric("9223372036854775807", SqlParserPos.ZERO);
    public static final String ARRAY_JOIN_PLACEHOLDER = "<$array_join$>";
    public static final String ARRAY_JOIN_REPLACE = "arrayJoin([-1, 1])";
    public static final SqlLiteral ZERO_SYS_OP_LITERAL = longLiteral(0L);
    public static final SqlLiteral ONE_SIGN_LITERAL = longLiteral(1L);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private AdqmDmlUtils() {
    }

    public static SqlNodeList getEntityColumnsList(Entity entity) {
        val columnList = new SqlNodeList(SqlParserPos.ZERO);
        entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .forEach(entityField -> columnList.add(identifier(entityField.getName())));
        fillSystemColumns(columnList);
        return columnList;
    }

    public static SqlIdentifier getActualTableIdentifier(String env, String datamart, String entityName) {
        return identifier(String.format("%s__%s", env, datamart), String.format("%s_actual", entityName));
    }

    public static SqlNodeList getInsertedColumnsList(List<EntityField> insertedColumns) {
        val columnList = new SqlNodeList(SqlParserPos.ZERO);
        for (EntityField insertedColumn : insertedColumns) {
            columnList.add(identifier(insertedColumn.getName()));
        }
        fillSystemColumns(columnList);
        return columnList;
    }

    public static SqlNodeList getSelectListForClose(Entity entity, long sysCn, SqlLiteral sysOpLiteral) {
        val selectList = new SqlNodeList(SqlParserPos.ZERO);
        entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .forEach(entityField -> selectList.add(identifier(TEMP_TABLE, entityField.getName())));
        selectList.add(identifier(TEMP_TABLE, SYS_FROM_FIELD));
        selectList.add(longLiteral(sysCn - 1));
        selectList.add(sysOpLiteral);

        val now = LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
        selectList.add(SqlLiteral.createTimestamp(new TimestampString(now), 0, SqlParserPos.ZERO));
        selectList.add(identifier(ARRAY_JOIN_PLACEHOLDER));
        return selectList;
    }

    public static void validatePrimaryKeys(List<EntityField> insertedColumns, List<String> pkFieldNames) {
        List<String> notFoundPrimaryKeys = new ArrayList<>(pkFieldNames.size());
        for (String pkFieldName : pkFieldNames) {
            boolean containsInColumns = insertedColumns.stream().anyMatch(entityField -> entityField.getName().equals(pkFieldName));
            if (!containsInColumns) {
                notFoundPrimaryKeys.add(pkFieldName);
            }
        }

        if (!notFoundPrimaryKeys.isEmpty()) {
            throw new DtmException(String.format("Inserted values must contain primary keys: %s", notFoundPrimaryKeys));
        }
    }

    public static QueryParameters extendParameters(QueryParameters queryParameters) {
        if (queryParameters == null) {
            return null;
        }

        val queryParamValues = queryParameters.getValues();
        val queryParamTypes = queryParameters.getTypes();
        val values = new ArrayList<>(queryParamValues.size() * 2);
        val types = new ArrayList<ColumnType>(queryParamTypes.size() * 2);
        values.addAll(queryParamValues);
        values.addAll(queryParamValues);
        types.addAll(queryParamTypes);
        types.addAll(queryParamTypes);
        return new QueryParameters(values, types);
    }

    private static void fillSystemColumns(SqlNodeList columnList) {
        columnList.add(identifier(SYS_FROM_FIELD));
        columnList.add(identifier(SYS_TO_FIELD));
        columnList.add(identifier(SYS_OP_FIELD));
        columnList.add(identifier(SYS_CLOSE_DATE_FIELD));
        columnList.add(identifier(SIGN_FIELD));
    }
}
