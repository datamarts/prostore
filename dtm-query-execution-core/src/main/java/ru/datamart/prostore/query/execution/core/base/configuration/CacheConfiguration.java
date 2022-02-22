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
package ru.datamart.prostore.query.execution.core.base.configuration;

import ru.datamart.prostore.cache.factory.CaffeineCacheServiceFactory;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.cache.*;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.delta.dto.HotDelta;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@EnableCaching
public class CacheConfiguration {

    public static final String CORE_QUERY_TEMPLATE_CACHE = "coreQueryTemplateCache";
    public static final String CORE_PREPARED_QUERY_CACHE = "corePreparedQueryCache";
    public static final String ENTITY_CACHE = "entity";
    public static final String HOT_DELTA_CACHE = "hotDelta";
    public static final String OK_DELTA_CACHE = "okDelta";
    public static final String MATERIALIZED_VIEW_CACHE = "materializedView";

    @Bean("entityCacheService")
    public CacheService<EntityKey, Entity> entityCacheService(@Qualifier("caffeineCacheManager")
                                                                      CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<EntityKey, Entity>(cacheManager)
                .create(ENTITY_CACHE);
    }

    @Bean("hotDeltaCacheService")
    public CacheService<String, HotDelta> hotDeltaCacheService(@Qualifier("caffeineCacheManager")
                                                                       CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, HotDelta>(cacheManager)
                .create(HOT_DELTA_CACHE);
    }

    @Bean("okDeltaCacheService")
    public CacheService<String, OkDelta> okDeltaCacheService(@Qualifier("caffeineCacheManager")
                                                                     CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, OkDelta>(cacheManager)
                .create(OK_DELTA_CACHE);
    }

    @Bean("materializedViewCacheService")
    public CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService(@Qualifier("caffeineCacheManager")
                                                                                                    CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<EntityKey, MaterializedViewCacheValue>(cacheManager)
                .create(MATERIALIZED_VIEW_CACHE);
    }

    @Bean("coreQueryTemplateCacheService")
    public CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService(@Qualifier("caffeineCacheManager")
                                                                                              CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<QueryTemplateKey, SourceQueryTemplateValue>(cacheManager)
                .create(CORE_QUERY_TEMPLATE_CACHE);
    }

    @Bean("evictQueryTemplateCacheServiceImpl")
    public EvictQueryTemplateCacheService evictQueryTemplateCacheService(
            CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService,
            List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList) {
        return new EvictQueryTemplateCacheService(cacheService, cacheServiceList);
    }
}
