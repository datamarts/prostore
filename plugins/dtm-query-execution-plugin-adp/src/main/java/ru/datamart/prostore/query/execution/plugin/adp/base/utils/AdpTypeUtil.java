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
package ru.datamart.prostore.query.execution.plugin.adp.base.utils;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.EntityField;

import java.util.Optional;

public class AdpTypeUtil {

    private AdpTypeUtil() {
    }

    public static String adpTypeFromDtmType(EntityField field) {
        return adpTypeFromDtmType(field.getType(), field.getSize());
    }

    public static String adpTypeFromDtmType(ColumnType type, Integer size) {
        switch (type) {
            case DATE:
                return "date";
            case TIME:
                return "time(6)";
            case TIMESTAMP:
                return "timestamp(6)";
            case FLOAT:
                return "float4";
            case DOUBLE:
                return "float8";
            case BOOLEAN:
                return "bool";
            case INT:
            case BIGINT:
                return "int8";
            case INT32:
                return "int4";
            case CHAR:
            case VARCHAR:
                return "varchar" + getVarcharSize(size);
            case UUID:
                return "varchar(36)";
            case LINK:
                return "varchar";
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", type));
        }
    }

    private static String getVarcharSize(Integer size) {
        return Optional.ofNullable(size)
                .map(sizeVal -> String.format("(%s)", sizeVal))
                .orElse("");
    }

}
