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
package ru.datamart.prostore.common.model.ddl;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Physical model of the service database field
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class EntityField implements Serializable {

    private final static Pattern nameWithSizePtn = Pattern.compile("\\w*(\\d)");

    private int ordinalPosition;
    private String name;
    private ColumnType type;
    private Integer size;
    private Integer accuracy;
    private Boolean nullable;
    private Integer primaryOrder;
    private Integer shardingOrder;
    private String defaultValue;


    public EntityField(int ordinalPosition,
                       String name,
                       String typeWithSize,
                       Boolean nullable,
                       Integer primaryOrder,
                       Integer shardingOrder,
                       String defaultValue) {
        this.name = name;
        this.nullable = nullable;
        this.primaryOrder = primaryOrder;
        this.shardingOrder = shardingOrder;
        this.defaultValue = defaultValue;
        parseType(typeWithSize);
        this.ordinalPosition = ordinalPosition;
    }

    public EntityField(int ordinalPosition,
                       String name,
                       ColumnType type,
                       Boolean isNull) {
        this.name = name;
        this.type = type;
        this.nullable = isNull;
        this.ordinalPosition = ordinalPosition;
    }

    public EntityField(int ordinalPosition,
                       String name,
                       String typeWithSize,
                       Boolean nullable,
                       String defaultValue) {
        this.name = name;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        parseType(typeWithSize);
        this.ordinalPosition = ordinalPosition;
    }

    private void parseType(String typeWithSize) {
        Matcher matcher = nameWithSizePtn.matcher(typeWithSize);
        if (matcher.find()) {
            this.size = Integer.parseInt(typeWithSize.substring(matcher.start(), matcher.end()));
            this.type = ColumnType.valueOf(typeWithSize.substring(0, matcher.start() - 1).toUpperCase());
        } else {
            this.type = ColumnType.valueOf(typeWithSize.toUpperCase());
        }
    }

    public EntityField copy() {
        return toBuilder().build();
    }

}
