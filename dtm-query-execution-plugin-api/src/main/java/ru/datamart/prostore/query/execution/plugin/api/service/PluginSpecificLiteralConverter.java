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
package ru.datamart.prostore.query.execution.plugin.api.service;

import lombok.val;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.util.List;

public interface PluginSpecificLiteralConverter {
    List<SqlNode> convert(List<SqlNode> params, List<SqlTypeName> parameterTypes);

    SqlNode convert(SqlNode param, SqlTypeName typeName);

    default String extractDateTimeString(SqlLiteral literal) {
        val value = literal.getValue();

        if (value.getClass() == NlsString.class) {
            return ((NlsString) value).getValue();
        }

        return value.toString();
    }
}
