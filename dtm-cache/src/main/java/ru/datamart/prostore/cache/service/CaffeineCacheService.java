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
package ru.datamart.prostore.cache.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CaffeineCacheService<K, V> implements CacheService<K, V> {
    protected final String cacheConfiguration;
    protected final CacheManager cacheManager;
    protected final Cache cache;

    public CaffeineCacheService(String cacheConfiguration, CacheManager cacheManager) {
        this.cacheConfiguration = cacheConfiguration;
        this.cacheManager = cacheManager;
        this.cache = Objects.requireNonNull(cacheManager.getCache(cacheConfiguration));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        val valueWrapper = cache.get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return ((Future<V>) valueWrapper.get()).result();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void forEach(BiConsumer<K, V> consumer) {
        ((CaffeineCache) cache).getNativeCache().asMap().forEach((k, v) -> {
            val key = (K) k;
            val value = ((Future<V>) v).result();
            consumer.accept(key, value);
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<V> getFuture(K key) {
        val valueWrapper = cache.get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return (Future<V>) valueWrapper.get();
        }
    }

    @Override
    public Future<V> put(K key, V value) {
        return Future.future(promise -> {
            cache.put(key, Future.succeededFuture(value));
            promise.complete(value);
        });
    }

    @Override
    public void remove(K key) {
        cache.evict(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeIf(Predicate<K> removeCondition) {
        final com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache =
                ((CaffeineCache) cache).getNativeCache();
        val byRemove = nativeCache.asMap().keySet().stream()
                .map(key -> (K) key)
                .filter(removeCondition)
                .collect(Collectors.toSet());
        byRemove.forEach(this::remove);
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
