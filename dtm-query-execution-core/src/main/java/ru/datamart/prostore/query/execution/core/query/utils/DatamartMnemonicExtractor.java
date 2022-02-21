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

import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.common.exception.DtmException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DatamartMnemonicExtractor {

    private final DeltaInformationExtractor deltaInformationExtractor;

    @Autowired
    public DatamartMnemonicExtractor(DeltaInformationExtractor deltaInformationExtractor) {
        this.deltaInformationExtractor = deltaInformationExtractor;
    }

    public String extract(SqlNode sqlNode) {
        val selectTree = new SqlSelectTree(sqlNode);
        val tables = selectTree.findAllTableAndSnapshots().stream()
                .map(node -> deltaInformationExtractor.getDeltaInformation(selectTree, node))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new DtmException("Tables or views not found in query");
        } else if (tables.stream().anyMatch(d -> Strings.isEmpty(d.getSchemaName()))) {
            throw new DtmException("Datamart must be specified for all tables and views");
        } else {
            val schemaName = tables.get(0).getSchemaName();
            log.debug("Extracted datamart [{}] from sql [{}]", schemaName, sqlNode);
            return schemaName;
        }
    }
}
