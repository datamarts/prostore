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
package ru.datamart.prostore.common.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.parser.SqlParserPos;

@AllArgsConstructor
@Data
@Builder
public class DeltaInformation {
    private final String tableAlias;
    private final String deltaTimestamp;
    private final boolean isLatestUncommittedDelta;
    private final DeltaType type;
    private Long selectOnNum;
    private SelectOnInterval selectOnInterval;
    private String schemaName;
    private String tableName;
    private SqlParserPos pos;

    public static DeltaInformation copy(DeltaInformation s) {
        return new DeltaInformation(
            s.tableAlias,
            s.deltaTimestamp,
            s.isLatestUncommittedDelta,
            s.type,
            s.selectOnNum,
            s.selectOnInterval,
            s.schemaName,
            s.tableName,
            s.pos);
    }
}
