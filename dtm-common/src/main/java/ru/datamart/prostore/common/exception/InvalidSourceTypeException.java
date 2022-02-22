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
package ru.datamart.prostore.common.exception;

import ru.datamart.prostore.common.reader.SourceType;

import java.util.Arrays;
import java.util.stream.Collectors;

public class InvalidSourceTypeException extends DtmException {

    private static final String AVAILABLE_SOURCE_TYPES = Arrays.stream(SourceType.values())
            .filter(SourceType::isAvailable)
            .map(SourceType::name)
            .collect(Collectors.joining(", "));

    private static final String PATTERN = "\"%s\" isn't a valid datasource type," +
            " please use one of the following: " + AVAILABLE_SOURCE_TYPES;

    public InvalidSourceTypeException(String sourceType) {
        super(String.format(PATTERN, sourceType));
    }

    public InvalidSourceTypeException(String sourceType, Throwable cause) {
        super(String.format(PATTERN, sourceType), cause);
    }
}
