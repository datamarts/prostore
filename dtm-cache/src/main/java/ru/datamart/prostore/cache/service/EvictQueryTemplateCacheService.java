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
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.function.Predicate;

public class EvictQueryTemplateCacheService {
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService;
    private final List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList;

    public EvictQueryTemplateCacheService(CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService,
                                          List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList) {
        this.cacheService = cacheService;
        this.cacheServiceList = cacheServiceList;
    }

    public void evictByDatamartName(String datamartName) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName));
    }

    public void evictByEntityName(String datamartName, String entityName) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName)
                && datamart.getEntities().stream()
                .anyMatch(dmEntity -> dmEntity.getName().equals(entityName)));
    }

    private void remove(Predicate<Datamart> predicate) {
        Predicate<QueryTemplateKey> templatePredicate = queryTemplateKey ->
                queryTemplateKey.getLogicalSchema().stream()
                        .anyMatch(predicate);
        cacheService.removeIf(templatePredicate);
        cacheServiceList.forEach(pluginCacheService -> pluginCacheService.removeIf(templatePredicate));
    }
}
