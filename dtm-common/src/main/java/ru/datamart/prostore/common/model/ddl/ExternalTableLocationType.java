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

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;

public enum ExternalTableLocationType {
    KAFKA,
    FILE,
    HDFS,
    CORE_ADB,
    CORE_ADP,
    CORE_ADG,
    CORE_ADQM;

    public static ExternalTableLocationType fromSourceType(SourceType type) {
        switch (type) {
            case ADG:
                return CORE_ADG;
            case ADB:
                return CORE_ADB;
            case ADP:
                return CORE_ADP;
            case ADQM:
                return CORE_ADQM;
            default:
                throw new DtmException(String.format("Invalid external table location of source type %s", type));
        }
    }
}
