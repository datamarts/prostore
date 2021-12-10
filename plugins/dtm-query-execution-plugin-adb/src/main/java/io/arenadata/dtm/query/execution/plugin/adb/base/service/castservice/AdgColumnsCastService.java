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
package io.arenadata.dtm.query.execution.plugin.adb.base.service.castservice;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adgColumnsCastService")
public class AdgColumnsCastService extends AbstractColumnsCastService {
    public AdgColumnsCastService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        super(sqlDialect);
    }

    @Override
    protected SqlNode surroundWith(ColumnType columnType, SqlTypeName sqlType, SqlNode nodeToSurround) {
        switch (columnType) {
            case DATE:
                return surroundDateNode(nodeToSurround, sqlType);
            case TIME:
                return surroundTimeNode(nodeToSurround, sqlType);
            case TIMESTAMP:
                return surroundTimestampNode(nodeToSurround, sqlType);
            default:
                throw new IllegalArgumentException("Invalid type to surround");
        }
    }

    @Override
    protected boolean isCastType(ColumnType logicalType) {
        switch (logicalType) {
            case TIMESTAMP:
            case TIME:
            case DATE:
                return true;
            default:
                return false;
        }
    }

}
