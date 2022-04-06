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
package ru.datamart.prostore.query.execution.core.base.utils;

import lombok.Getter;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.EntityType;

@Getter
public enum InformationSchemaEntityType {
    BASE_TABLE("BASE TABLE"),
    VIEW("VIEW"),
    MATERIALIZED_VIEW("MATERIALIZED VIEW"),
    WRITABLE_EXTERNAL_TABLE("WRITABLE EXTERNAL TABLE"),
    READABLE_EXTERNAL_TABLE("READABLE EXTERNAL TABLE");

    private final String name;

    InformationSchemaEntityType(String name) {
        this.name = name;
    }

    public static InformationSchemaEntityType getByEntityType(EntityType entityType) {
        switch (entityType) {
            case TABLE:
                return BASE_TABLE;
            case WRITEABLE_EXTERNAL_TABLE:
                return WRITABLE_EXTERNAL_TABLE;
            case READABLE_EXTERNAL_TABLE:
                return READABLE_EXTERNAL_TABLE;
            case VIEW:
                return VIEW;
            case MATERIALIZED_VIEW:
                return MATERIALIZED_VIEW;
            default:
                throw new DtmException(String.format("Unexpected entity type %s", entityType));
        }
    }
}
