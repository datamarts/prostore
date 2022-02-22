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
package ru.datamart.prostore.query.execution.plugin.adp.base.configuration;

import ru.datamart.prostore.cache.factory.CaffeineCacheServiceFactory;
import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.common.cache.QueryTemplateKey;
import ru.datamart.prostore.common.cache.QueryTemplateValue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static ru.datamart.prostore.query.execution.plugin.adp.base.service.AdpDtmDataSourcePlugin.ADP_DATAMART_CACHE;
import static ru.datamart.prostore.query.execution.plugin.adp.base.service.AdpDtmDataSourcePlugin.ADP_QUERY_TEMPLATE_CACHE;


@Configuration
public class CacheConfiguration {

    @Bean("adpQueryTemplateCacheService")
    public CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService(@Qualifier("caffeineCacheManager")
                                                                                        CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<QueryTemplateKey, QueryTemplateValue>(cacheManager)
                .create(ADP_QUERY_TEMPLATE_CACHE);
    }

    @Bean("adpDatamartCacheService")
    public CacheService<String, String> datamartCacheService(@Qualifier("caffeineCacheManager")
                                                                     CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, String>(cacheManager)
                .create(ADP_DATAMART_CACHE);
    }

}
