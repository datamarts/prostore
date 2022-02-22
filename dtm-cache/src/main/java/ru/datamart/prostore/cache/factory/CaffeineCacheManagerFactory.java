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
package ru.datamart.prostore.cache.factory;

import com.github.benmanes.caffeine.cache.Caffeine;
import ru.datamart.prostore.cache.configuration.CacheProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;

import java.util.concurrent.TimeUnit;

public class CaffeineCacheManagerFactory implements CacheManagerFactory {

    @Override
    public CaffeineCacheManager create(CacheProperties cacheProperties) {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(caffeineCacheBuilder(cacheProperties));
        return cacheManager;
    }

    private Caffeine<Object, Object> caffeineCacheBuilder(CacheProperties cacheProperties) {
        return Caffeine.newBuilder()
                .initialCapacity(cacheProperties.getInitialCapacity())
                .maximumSize(cacheProperties.getMaximumSize())
                .expireAfterAccess(cacheProperties.getExpireAfterAccessMinutes(), TimeUnit.MINUTES)
                .recordStats();
    }
}
