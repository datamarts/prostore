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
package ru.datamart.prostore.query.calcite.core.service;

import ru.datamart.prostore.common.reader.QueryTemplateResult;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface QueryTemplateExtractor {
    QueryTemplateResult extract(SqlNode sqlNode);

    QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns);

    QueryTemplateResult extract(String sql);

    QueryTemplateResult extract(String sql, List<String> excludeColumns);

    SqlNode enrichTemplate(SqlNode templateNode, List<SqlNode> params);
}
