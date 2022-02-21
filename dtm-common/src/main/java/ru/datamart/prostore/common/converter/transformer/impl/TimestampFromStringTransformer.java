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
package ru.datamart.prostore.common.converter.transformer.impl;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.val;
import ru.datamart.prostore.common.converter.transformer.AbstractColumnTransformer;
import ru.datamart.prostore.common.util.DateTimeUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;

@AllArgsConstructor
@NoArgsConstructor
public class TimestampFromStringTransformer extends AbstractColumnTransformer<Long, String> {

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Override
    public Long transformValue(String value) {
        if (value == null) {
            return null;
        }

        val localDateTime = LocalDateTime.parse(value, dateTimeFormatter);
        return DateTimeUtils.toMicros(localDateTime);
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(String.class);
    }
}
