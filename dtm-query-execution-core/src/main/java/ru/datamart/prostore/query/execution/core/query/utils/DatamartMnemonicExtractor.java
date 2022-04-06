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
package ru.datamart.prostore.query.execution.core.query.utils;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;

@Slf4j
public final class DatamartMnemonicExtractor {

    private DatamartMnemonicExtractor() {
    }

    public static String extract(SqlNode sqlNode) {
        val tables = new SqlSelectTree(sqlNode).findAllTableAndSnapshots();
        if (tables.isEmpty()) {
            throw new DtmException("Tables or views not found in query");
        }

        String result = null;
        for (val table : tables) {
            val schemaOptional = table.tryGetSchemaName().filter(StringUtils::isNotBlank);
            if (!schemaOptional.isPresent()) {
                throw new DtmException("Datamart must be specified for all tables and views");
            }

            if (result == null) {
                val schema = schemaOptional.get();
                result = schema;
                log.debug("Extracted datamart [{}] from sql [{}]", schema, sqlNode);
            }
        }

        return result;
    }
}
