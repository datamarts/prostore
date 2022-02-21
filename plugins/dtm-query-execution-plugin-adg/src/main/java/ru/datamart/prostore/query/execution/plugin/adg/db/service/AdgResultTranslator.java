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
package ru.datamart.prostore.query.execution.plugin.adg.db.service;

import ru.datamart.prostore.query.execution.plugin.adg.base.exception.DtmTarantoolException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service("adgResultTranslator")
public class AdgResultTranslator {
    public List<Object> translate(Object result) {
        try {
            val list = (List<Object>) result;
            if (list.get(0) != null) {
                return list;
            }

            log.error("[ADG] Error result: {}", result);
            val error = list.get(1);

            if (error instanceof String) {
                throw new DtmTarantoolException((String) error);
            }

            if (error instanceof Map<?, ?>) {
                val errorMap = (Map<?, ?>) error;
                throw new DtmTarantoolException(errorMap.toString());
            }

            throw new DtmTarantoolException("Unknown type of error: " + error);
        } catch (Exception e) {
            log.error("[ADG] Unexpected error during result translate: {}", result);
            throw new DtmTarantoolException("Adg result can't be processed: " + result, e);
        }
    }
}
