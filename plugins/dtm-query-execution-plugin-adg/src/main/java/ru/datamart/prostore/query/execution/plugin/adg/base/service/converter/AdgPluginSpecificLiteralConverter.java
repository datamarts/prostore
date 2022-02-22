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
package ru.datamart.prostore.query.execution.plugin.adg.base.service.converter;

import ru.datamart.prostore.common.util.DateTimeUtils;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

@Service("adgTemplateParameterConverter")
public class AdgPluginSpecificLiteralConverter implements PluginSpecificLiteralConverter {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();


    @Override
    public List<SqlNode> convert(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        List<SqlNode> nwParams = new ArrayList<>();
        for (int i = 0; i < params.size(); i++) {
            nwParams.add(convert(params.get(i), parameterTypes.get(i)));
        }
        return nwParams;
    }

    @Override
    public SqlNode convert(SqlNode param, SqlTypeName typeName) {
        if (SqlKind.DYNAMIC_PARAM.equals(param.getKind())) {
            return param;
        }

        val literal = (SqlLiteral) param;
        if (literal.getValue() == null) {
            return SqlLiteral.createNull(SqlParserPos.ZERO);
        }

        switch (typeName) {
            case TIME:
                val localTime = LocalTime.parse(extractDateTimeString(literal));
                return SqlLiteral.createExactNumeric(String.valueOf(DateTimeUtils.toMicros(localTime)), param.getParserPosition());
            case DATE:
                val localDate = LocalDate.parse(extractDateTimeString(literal), DateTimeFormatter.ISO_LOCAL_DATE);
                return SqlLiteral.createExactNumeric(String.valueOf(DateTimeUtils.toEpochDay(localDate)), param.getParserPosition());
            case TIMESTAMP:
                val dateTime = LocalDateTime.parse(extractDateTimeString(literal), TIMESTAMP_FORMATTER);
                return SqlLiteral.createExactNumeric(String.valueOf(DateTimeUtils.toMicros(dateTime)), param.getParserPosition());
            default:
                return param;
        }
    }

    public List<SqlNode> convertDeleteParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        List<SqlNode> nwParams = new ArrayList<>();
        for (int i = 0; i < params.size(); i++) {
            nwParams.add(convertDeleteParam(params.get(i), parameterTypes.get(i)));
        }
        return nwParams;
    }

    public SqlNode convertDeleteParam(SqlNode param, SqlTypeName typeName) {
        if (SqlKind.DYNAMIC_PARAM.equals(param.getKind())) {
            return param;
        }

        val literal = (SqlLiteral) param;
        if (literal.getValue() == null) {
            return SqlLiteral.createNull(SqlParserPos.ZERO);
        }

        switch (typeName) {
            case TIME:
                val localTime = LocalTime.parse(extractDateTimeString(literal));
                return SqlLiteral.createCharString(String.valueOf(DateTimeUtils.toMicros(localTime)), param.getParserPosition());
            case DATE:
                val localDate = LocalDate.parse(extractDateTimeString(literal), DateTimeFormatter.ISO_LOCAL_DATE);
                return SqlLiteral.createCharString(String.valueOf(DateTimeUtils.toEpochDay(localDate)), param.getParserPosition());
            case TIMESTAMP:
                val dateTime = LocalDateTime.parse(extractDateTimeString(literal), TIMESTAMP_FORMATTER);
                return SqlLiteral.createCharString(String.valueOf(DateTimeUtils.toMicros(dateTime)), param.getParserPosition());
            default:
                return param;
        }
    }
}
