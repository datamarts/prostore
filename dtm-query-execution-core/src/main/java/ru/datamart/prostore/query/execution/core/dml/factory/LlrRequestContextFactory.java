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
package ru.datamart.prostore.query.execution.core.dml.factory;

import ru.datamart.prostore.common.cache.SourceQueryTemplateValue;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.reader.QuerySourceRequest;
import ru.datamart.prostore.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.core.dml.dto.DmlRequestContext;
import ru.datamart.prostore.query.execution.core.dml.dto.LlrRequestContext;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LlrRequestContextFactory {

    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;
    private final QueryParserService parserService;

    @Autowired
    public LlrRequestContextFactory(LogicalSchemaProvider logicalSchemaProvider,
                                    ColumnMetadataService columnMetadataService,
                                    CoreCalciteDMLQueryParserService parserService) {
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.parserService = parserService;
    }

    public Future<LlrRequestContext> create(DmlRequestContext context, DeltaQueryPreprocessorResponse deltaResponse) {
        val llrContext = createLlrRequestContext(context);
        llrContext.setDeltaInformations(deltaResponse.getDeltaInformations());
        llrContext.setCachable(deltaResponse.isCachable());
        llrContext.getDmlRequestContext().setSqlNode(deltaResponse.getSqlNode());
        return initLlrContext(llrContext);
    }

    public Future<LlrRequestContext> create(DmlRequestContext context, SourceQueryTemplateValue queryTemplateValue) {
        val llrContext = createLlrRequestContext(context);
        llrContext.getSourceRequest().setMetadata(queryTemplateValue.getMetadata());
        llrContext.getSourceRequest().setLogicalSchema(queryTemplateValue.getLogicalSchema());
        llrContext.getSourceRequest().getQueryRequest().setSql(queryTemplateValue.getSql());
        llrContext.setQueryTemplateValue(queryTemplateValue);
        return Future.succeededFuture(llrContext);
    }

    private LlrRequestContext createLlrRequestContext(DmlRequestContext context) {
        val sourceRequest = new QuerySourceRequest(context.getRequest().getQueryRequest(),
                context.getSqlNode(),
                context.getSourceType());
        return LlrRequestContext.builder()
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
    }

    private Future<LlrRequestContext> initLlrContext(LlrRequestContext llrContext) {
        return logicalSchemaProvider.getSchemaFromQuery(
                llrContext.getDmlRequestContext().getSqlNode(),
                llrContext.getDmlRequestContext().getRequest().getQueryRequest().getDatamartMnemonic())
                .map(schema -> {
                    llrContext.getSourceRequest().setLogicalSchema(schema);
                    return schema;
                })
                .compose(schema -> parserService.parse(new QueryParserRequest(llrContext.getDmlRequestContext().getSqlNode(),
                        schema)))
                .map(response -> {
                    llrContext.setRelNode(response.getRelNode());
                    return response.getRelNode();
                })
                .compose(columnMetadataService::getColumnMetadata)
                .map(metadata -> {
                    llrContext.getSourceRequest().setMetadata(metadata);
                    return llrContext;
                });
    }
}
