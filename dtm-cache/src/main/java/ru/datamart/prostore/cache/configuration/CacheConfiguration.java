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
package ru.datamart.prostore.cache.configuration;

import ru.datamart.prostore.cache.factory.CacheManagerFactory;
import ru.datamart.prostore.cache.factory.CaffeineCacheManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration()
@DependsOn({"cacheProperties"})
public class CacheConfiguration {

    @Bean("caffeineCacheManagerFactory")
    public CaffeineCacheManagerFactory caffeineCacheManagerFactory() {
        return new CaffeineCacheManagerFactory();
    }

    @Bean("caffeineCacheManager")
    public CacheManager caffeineCacheManager(@Qualifier("caffeineCacheManagerFactory") CacheManagerFactory caffeineCacheManagerFactory,
                                             @Qualifier("cacheProperties") CacheProperties cacheProperties) {
        return caffeineCacheManagerFactory.create(cacheProperties);
    }

}
