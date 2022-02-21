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
package ru.datamart.prostore.query.execution.core.query.utils;

import ru.datamart.prostore.common.exception.DtmException;
import org.apache.commons.lang3.StringUtils;

public final class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static String prepareMessage(Throwable failure) {
        if (failure == null) {
            return null;
        }

        if (failure instanceof DtmException) {
            if (StringUtils.isNotBlank(failure.getMessage())) {
                return failure.getMessage();
            } else {
                return String.format("Error: %s", failure.getClass().getSimpleName());
            }
        } else if (StringUtils.isNotBlank(failure.getMessage())) {
            return String.format("Unexpected error: %s : %s", failure.getClass().getSimpleName(), failure.getMessage());
        }

        return String.format("Unexpected error: %s", failure.getClass().getSimpleName());
    }
}
