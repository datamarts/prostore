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
package ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service;

import ru.datamart.prostore.common.calcite.CalciteContext;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Dml query modifier
 */
public interface QueryGenerator {
    /**
     * modify query
     */
    Future<String> mutateQuery(RelRoot relNode,
                               List<DeltaInformation> deltaInformations,
                               CalciteContext calciteContext,
                               EnrichQueryRequest enrichQueryRequest);

    Future<SqlNode> getMutatedSqlNode(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest);
}
