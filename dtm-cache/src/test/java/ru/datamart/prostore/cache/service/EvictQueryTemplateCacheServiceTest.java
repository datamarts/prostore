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
package ru.datamart.prostore.cache.service;

import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import ru.datamart.prostore.common.cache.SourceQueryTemplateValue;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EvictQueryTemplateCacheServiceTest {
    private static final String USED_SCHEMA = "used_schema";
    private static final String USED_SCHEMA_TABLE = "used_schema_table";
    private static final String USED_SCHEMA_VIEW = "used_schema_view";
    private static final String TEMPLATE_1 = "template_1";
    private static final String TEMPLATE_2 = "template_2";
    private static final String TEMPLATE_3 = "template_3";
    private static final String TEMPLATE_4 = "template_4";
    private static final String TEMPLATE_5 = "template_5";
    private static final List<QueryTemplateKey> CACHE_LIST = Arrays.asList(
            getTemplate(TEMPLATE_1, USED_SCHEMA,
                    Collections.singletonList(getEntity(USED_SCHEMA_TABLE, EntityType.TABLE))),
            getTemplate(TEMPLATE_2, USED_SCHEMA,
                    Collections.singletonList(getEntity("table_1", EntityType.TABLE))),
            getTemplate(TEMPLATE_3, USED_SCHEMA,
                    Collections.singletonList(getEntity(USED_SCHEMA_VIEW, EntityType.VIEW))),
            getTemplate(TEMPLATE_4, USED_SCHEMA,
                    Arrays.asList(getEntity(USED_SCHEMA_VIEW, EntityType.VIEW),
                            getEntity(USED_SCHEMA_TABLE, EntityType.TABLE))),
            getTemplate(TEMPLATE_5, "not_used_schema",
                    Arrays.asList(getEntity(USED_SCHEMA_TABLE, "not_used_schema", EntityType.TABLE),
                            getEntity(USED_SCHEMA_VIEW, "not_used_schema", EntityType.VIEW)))
    );

    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService = mock(CacheService.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> adbCacheService = mock(CacheService.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> adgCacheService = mock(CacheService.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> adqmCacheService = mock(CacheService.class);
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService =
            new EvictQueryTemplateCacheService(cacheService,
                    Arrays.asList(adbCacheService, adgCacheService, adqmCacheService));

    @BeforeEach
    void init() {
        doNothing().when(cacheService).removeIf(any());
        doNothing().when(adbCacheService).removeIf(any());
        doNothing().when(adgCacheService).removeIf(any());
        doNothing().when(adqmCacheService).removeIf(any());
    }

    @Test
    void testEvictByDatamartName() {
        evictQueryTemplateCacheService.evictByDatamartName(USED_SCHEMA);
        validate(Arrays.asList(TEMPLATE_1, TEMPLATE_2, TEMPLATE_3, TEMPLATE_4));
    }

    @Test
    void testEvictByDatamartNameWithWrongName() {
        evictQueryTemplateCacheService.evictByDatamartName("wrong_name");
        validate(Collections.emptyList());
    }

    @Test
    void testEvictByEntityNameTable() {
        evictQueryTemplateCacheService.evictByEntityName(USED_SCHEMA, USED_SCHEMA_TABLE);
        validate(Arrays.asList(TEMPLATE_1, TEMPLATE_4));
    }

    private void validate(List<String> expectedTemplateList) {
        validate(cacheService, expectedTemplateList);
        validate(adbCacheService, expectedTemplateList);
        validate(adgCacheService, expectedTemplateList);
        validate(adqmCacheService, expectedTemplateList);
    }

    private void validate(CacheService<QueryTemplateKey, ?> cacheService, List<String> expectedTemplateList) {
        ArgumentCaptor<Predicate<QueryTemplateKey>> captor = ArgumentCaptor.forClass(Predicate.class);
        verify(cacheService, times(1)).removeIf(captor.capture());
        List<String> deletedTemplates = CACHE_LIST.stream()
                .filter(captor.getValue())
                .map(QueryTemplateKey::getSourceQueryTemplate)
                .collect(Collectors.toList());
        assertEquals(expectedTemplateList, deletedTemplates);
    }
    private static QueryTemplateKey getTemplate(String template, String datamart, List<Entity> entities) {
        return QueryTemplateKey
                .builder()
                .sourceQueryTemplate(template)
                .logicalSchema(Collections.singletonList(new Datamart(datamart, false, entities)))
                .build();
    }

    private static Entity getEntity(String name, EntityType type) {
        return getEntity(name, USED_SCHEMA, type);
    }

    private static Entity getEntity(String name, String schema, EntityType type) {
        return Entity.builder()
                .name(name)
                .entityType(type)
                .schema(schema)
                .fields(Collections.emptyList())
                .build();
    }
}
