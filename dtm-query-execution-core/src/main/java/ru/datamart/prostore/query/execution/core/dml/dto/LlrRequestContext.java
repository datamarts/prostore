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
package ru.datamart.prostore.query.execution.core.dml.dto;

import ru.datamart.prostore.common.cache.SourceQueryTemplateValue;
import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.reader.QuerySourceRequest;
import ru.datamart.prostore.common.reader.SourceType;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

@Data
public class LlrRequestContext {
    private List<DeltaInformation> deltaInformations;
    private DmlRequestContext dmlRequestContext;
    private QuerySourceRequest sourceRequest;
    private SourceQueryTemplateValue queryTemplateValue;
    private RelRoot relNode;
    private SqlNode originalQuery;
    private boolean cachable = true;
    private SourceType executionPlugin;
    private List<SqlTypeName> parameterTypes;

    @Builder
    public LlrRequestContext(List<DeltaInformation> deltaInformations,
                             DmlRequestContext dmlRequestContext,
                             QuerySourceRequest sourceRequest,
                             SqlNode originalQuery) {
        this.deltaInformations = deltaInformations;
        this.dmlRequestContext = dmlRequestContext;
        this.sourceRequest = sourceRequest;
        this.originalQuery = originalQuery;
    }
}
