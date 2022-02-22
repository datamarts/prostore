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
package ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema;

import ru.datamart.prostore.common.model.ddl.ColumnType;

/**
 * Conversion from physical type to Tarantool type
 */
public class SpaceAttributeTypeUtil {

    public static SpaceAttributeTypes toAttributeType(ColumnType type) {
        switch (type) {
            case UUID:
            case CHAR:
            case VARCHAR:
            case LINK:
            case ANY:
                return SpaceAttributeTypes.STRING;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case INT32:
            case INT:
            case BIGINT:
                return SpaceAttributeTypes.INTEGER;
            case FLOAT:
            case DOUBLE:
                return SpaceAttributeTypes.NUMBER;
            case BOOLEAN:
                return SpaceAttributeTypes.BOOLEAN;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", type));
        }
    }

    private SpaceAttributeTypeUtil() {
    }
}
