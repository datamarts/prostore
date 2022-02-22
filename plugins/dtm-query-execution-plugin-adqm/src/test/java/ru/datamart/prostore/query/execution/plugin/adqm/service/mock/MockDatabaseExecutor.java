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
package ru.datamart.prostore.query.execution.plugin.adqm.service.mock;

import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class MockDatabaseExecutor implements DatabaseExecutor {
    private final List<Predicate<String>> expectedCalls;
    private final Map<Predicate<String>, List<Map<String, Object>>> mockData;
    private int callCount;
    private boolean isStrictOrder;

    public MockDatabaseExecutor(final List<Predicate<String>> expectedCalls,
                                final Map<Predicate<String>, List<Map<String, Object>>> mockData,
                                boolean isStrictOrder) {
        this.expectedCalls = expectedCalls;
        this.mockData = mockData;
        this.isStrictOrder = isStrictOrder;
    }

    public MockDatabaseExecutor(final List<Predicate<String>> expectedCalls) {
        this(expectedCalls, Collections.emptyMap(), true);
    }

    public MockDatabaseExecutor(final List<Predicate<String>> expectedCalls,
                                final Map<Predicate<String>, List<Map<String, Object>>> mockData) {
        this(expectedCalls, mockData, true);
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            // if we provide results, this calls are not treated as expected
            val result = findResult(sql);
            if (result.isPresent()) {
                promise.complete(result.get());
                return;
            }

            val r = call(sql);
            if (r.getLeft()) {
                promise.complete();
            } else {
                System.out.println(r.getRight());
                promise.fail(new DataSourceException(r.getRight()));
            }
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            val r = call(sql);
            if (r.getLeft()) {
                promise.complete();
            } else {
                promise.fail(new DataSourceException(r.getRight()));
            }
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            val r = call(sql);
            if (r.getLeft()) {
                promise.complete();
            } else {
                promise.fail(new DataSourceException(r.getRight()));
            }
        });
    }

    public List<Predicate<String>> getExpectedCalls() {
        return Collections.unmodifiableList(expectedCalls);
    }

    public int getCallCount() {
        return callCount;
    }

    private Pair<Boolean, String> call(String sql) {
        callCount++;
        if (callCount > expectedCalls.size()) {
            return Pair.of(false, String.format("Extra call. Expected %d, got %d", expectedCalls.size(), callCount));
        }

        if (isStrictOrder) {
            Predicate<String> expected = expectedCalls.get(callCount - 1);
            return expected.test(sql) ? Pair.of(true, "")
                    : Pair.of(false, String.format("Unexpected SQL: %s", sql));
        } else {
            return expectedCalls.stream().filter(e -> e.test(sql)).findFirst()
                    .map(v -> Pair.of(true, ""))
                    .orElse(Pair.of(false, String.format("Unexpected SQL: %s", sql)));
        }
    }

    private Optional<List<Map<String, Object>>> findResult(String sql) {
        for (val e : mockData.entrySet()) {
            if (e.getKey().test(sql)) {
                return Optional.of(e.getValue());
            }
        }

        return Optional.empty();
    }
}
