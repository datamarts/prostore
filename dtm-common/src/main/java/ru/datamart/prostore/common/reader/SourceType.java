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
package ru.datamart.prostore.common.reader;

import ru.datamart.prostore.common.exception.InvalidSourceTypeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;

/**
 * Data source type
 */

@NoArgsConstructor
@AllArgsConstructor
public enum SourceType {
    ADB,
    ADG,
    ADQM,
    ADP,
    INFORMATION_SCHEMA(false);

    @Getter
    private boolean isAvailable = true;

    public static SourceType valueOfAvailable(String typeName) {
        return Arrays.stream(SourceType.values())
                .filter(type -> type.isAvailable() && type.name().equalsIgnoreCase(typeName))
                .findAny()
                .orElseThrow(() -> new InvalidSourceTypeException(typeName));
    }
}
