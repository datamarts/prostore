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
package ru.datamart.prostore.cache.factory;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.CaffeineCacheService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;

import java.util.Collections;

public class CaffeineCacheServiceFactory<K, V> implements CacheServiceFactory<K, V> {

    private final CacheManager cacheManager;

    public CaffeineCacheServiceFactory(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public CacheService<K, V> create(String cacheConfiguration) {
        CaffeineCacheManager caffeineCacheManager = (CaffeineCacheManager) this.cacheManager;
        caffeineCacheManager.setCacheNames(Collections.singleton(cacheConfiguration));
        return new CaffeineCacheService<>(cacheConfiguration, this.cacheManager);
    }
}
