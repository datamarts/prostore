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

import com.github.benmanes.caffeine.cache.Cache;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class CaffeineCacheServiceTest {

    private static final CacheManager cacheManager = mock(CacheManager.class);
    private static final CaffeineCache cache = mock(CaffeineCache.class);
    private static final Cache<Object, Object> nativeCache = (Cache<Object, Object>) mock(Cache.class);
    private static final ArgumentCaptor<Future<String>> futureStringArgumentCaptor = ArgumentCaptor.forClass(Future.class);

    private static CaffeineCacheService<Integer, String> caffeineCacheService;

    @BeforeAll
    static void setUp() {
    }

    @Test
    void executePut(VertxTestContext testContext) {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        val cacheKey = 0;
        val cacheValue = "TEST";
        doNothing().when(cache).put(any(), futureStringArgumentCaptor.capture());

        //act
        caffeineCacheService.put(cacheKey, cacheValue)
                .onComplete(value -> testContext.verify(() -> {
                    //assert
                    assertEquals(cacheValue, value.result());
                    assertEquals(cacheValue, futureStringArgumentCaptor.getValue().result());
                }).completeNow());
    }

    @Test
    void executeGet() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        val cacheKey = 0;
        val cacheValue = "TEST";
        val valueWrapper = new SimpleValueWrapper(Future.succeededFuture(cacheValue));
        when(cache.get(cacheKey)).thenReturn(valueWrapper);

        //act
        val resultCacheValue = caffeineCacheService.get(cacheKey);

        //assert
        assertEquals(cacheValue, resultCacheValue);
    }

    @Test
    void executeGetNull() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        val cacheKey = 0;
        when(cache.get(cacheKey)).thenReturn(null);

        //act
        val resultCacheValue = caffeineCacheService.get(cacheKey);

        //assert
        assertNull(resultCacheValue);
    }

    @Test
    void executeGetFuture() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        val cacheKey = 0;
        val cacheValue = Future.succeededFuture("TEST");
        val valueWrapper = new SimpleValueWrapper(cacheValue);
        when(cache.get(cacheKey)).thenReturn(valueWrapper);

        //act
        val resultCacheValue = caffeineCacheService.getFuture(cacheKey);

        //assert
        assertEquals(cacheValue, resultCacheValue);
    }

    @Test
    void executeGetFutureNull() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        val cacheKey = 0;
        when(cache.get(cacheKey)).thenReturn(null);

        //act
        val resultCacheValue = caffeineCacheService.getFuture(cacheKey);

        //assert
        assertNull(resultCacheValue);
    }

    @Test
    void executeForEach() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(new CaffeineCache("CACHE", nativeCache));
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        Map<Integer, String> resultMap = new HashMap<>();
        ConcurrentMap<Object, Object> concurrentMap = new ConcurrentHashMap<>();
        for (int i = 0; i < 3; i++) {
            concurrentMap.put(i, Future.succeededFuture("TEST" + i));
        }
        when(nativeCache.asMap()).thenReturn(concurrentMap);

        //act
        caffeineCacheService.forEach(resultMap::put);

        //assert
        resultMap.forEach((key, value) ->
                assertEquals(value, ((Future<String>) concurrentMap.get(key)).result()));
    }

    @Test
    void executeRemoveIf() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(new CaffeineCache("CACHE", nativeCache));
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);
        ConcurrentMap<Object, Object> concurrentMap = new ConcurrentHashMap<>();
        for (int i = 0; i < 3; i++) {
            concurrentMap.put(i, Future.succeededFuture("TEST" + i));
        }
        when(nativeCache.asMap()).thenReturn(concurrentMap);

        //act
        caffeineCacheService.removeIf(key -> key < 2);

        //assert
        verify(nativeCache).invalidate(0);
        verify(nativeCache).invalidate(1);
    }

    @Test
    void executeRemove() {
        //arrange
        val cacheKey = 0;
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);

        //act
        caffeineCacheService.remove(cacheKey);

        //assert
        verify(cache).evict(cacheKey);
    }

    @Test
    void executeClear() {
        //arrange
        when(cacheManager.getCache(anyString())).thenReturn(cache);
        caffeineCacheService = new CaffeineCacheService<>("TEST_CACHE", cacheManager);

        //act
        caffeineCacheService.clear();

        //assert
        verify(cache).clear();
    }
}
