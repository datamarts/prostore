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
package ru.datamart.prostore.query.calcite.core.visitors;

import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashSet;
import java.util.Set;

public class SqlInvalidTimestampFinder extends SqlBasicVisitor<Object> {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private static final String TIMESTAMP_TYPE_NAME = "TIMESTAMP";
    private final Set<String> invalidTimestamps = new HashSet<>();

    @Override
    public Object visit(SqlCall call) {
        if (SqlKind.CAST.equals(call.getKind())) {
            val sqlBasicCall = (SqlBasicCall) call;
            if (((SqlDataTypeSpec) sqlBasicCall.getOperands()[1]).getTypeName().getSimple().equals(TIMESTAMP_TYPE_NAME)) {
                val value = ((SqlCharStringLiteral) sqlBasicCall.getOperands()[0]).toValue();
                try {
                    TIMESTAMP_FORMATTER.parse(value);
                } catch (Exception e) {
                    invalidTimestamps.add(value);
                }
            }
        }
        return super.visit(call);
    }

    public Set<String> getInvalidTimestamps() {
        return invalidTimestamps;
    }
}
