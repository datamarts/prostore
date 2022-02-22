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
package ru.datamart.prostore.common.reader;

/**
 * Запрос с обогащенным запросом DML
 */
public class EnrichedQueryRequest {

  /**
   * Исходный запрос
   */
  private QueryRequest queryRequest;

  /**
   * Обогащенный запрос
   */
  private String enrichSql;

  public EnrichedQueryRequest() {
  }

  public EnrichedQueryRequest(QueryRequest queryRequest, String enrichSql) {
    this.queryRequest = queryRequest;
    this.enrichSql = enrichSql;
  }

  public QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public void setQueryRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  public String getEnrichSql() {
    return enrichSql;
  }

  public void setEnrichSql(String enrichSql) {
    this.enrichSql = enrichSql;
  }
}
