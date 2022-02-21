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
package ru.datamart.prostore.common.converter.transformer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ColumnTransformer {

    static Map<Class<?>, ColumnTransformer> getTransformerMap(ColumnTransformer... columnTransformers) {
        Map<Class<?>, ColumnTransformer> map = new HashMap<>();
        for (ColumnTransformer transformer : columnTransformers) {
            map.putAll(transformer.getTransformClasses().stream()
                .collect(Collectors.toMap(Function.identity(), tc -> transformer)));
        }
        return map;
    }

    Object transform(Object value);

    Collection<Class<?>> getTransformClasses();
}
