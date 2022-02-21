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
package ru.datamart.prostore.jdbc.protocol.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.datamart.prostore.jdbc.core.QueryRequest;
import ru.datamart.prostore.jdbc.core.QueryResult;
import ru.datamart.prostore.jdbc.model.ColumnInfo;
import ru.datamart.prostore.jdbc.model.SchemaInfo;
import ru.datamart.prostore.jdbc.model.TableInfo;
import ru.datamart.prostore.jdbc.protocol.Protocol;
import ru.datamart.prostore.jdbc.util.DtmSqlException;
import ru.datamart.prostore.jdbc.util.ResponseException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.jdbc.protocol.http.MapperUtils.configureMapper;
import static ru.datamart.prostore.jdbc.util.DriverConstants.HOST_PROPERTY;
import static org.apache.http.util.TextUtils.isEmpty;

/**
 * Http implementation of data reader service
 */
@Slf4j
public class HttpReaderService implements Protocol {
    private static final TypeReference<List<SchemaInfo>> DATABASE_SCHEMAS_TYPE = new TypeReference<List<SchemaInfo>>() {
    };
    private static final TypeReference<List<TableInfo>> TABLE_INFOS_TYPE = new TypeReference<List<TableInfo>>() {
    };
    private static final TypeReference<List<ColumnInfo>> DATABASE_COLUMNS_TYPE = new TypeReference<List<ColumnInfo>>() {
    };
    private static final String GET_META_URL = "/meta";
    private static final String GET_ENTITIES_URL = "/meta/%s/entities";
    private static final String GET_ATTRIBUTES_URL = "/meta/%s/entity/%s/attributes";
    private static final ObjectMapper MAPPER = configureMapper();
    private final CloseableHttpClient client;
    private final String backendHostUrl;

    @SneakyThrows
    public HttpReaderService(CloseableHttpClient client, String dbHost) {
        if (isEmpty(dbHost)) {
            throw new DtmSqlException(String.format("Unable to create connection because parameter '%s' is not specified", HOST_PROPERTY));
        }
        this.backendHostUrl = "http://" + dbHost;
        this.client = client;
    }

    @Override
    public List<SchemaInfo> getDatabaseSchemas() {
        try {
            HttpGet httpGet = new HttpGet(backendHostUrl + GET_META_URL);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, DATABASE_SCHEMAS_TYPE);
            }
        } catch (IOException e) {
            log.error("Error loading database schemas.", e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public List<TableInfo> getDatabaseTables(String schemaPattern) {
        try {
            String uri = String.format(backendHostUrl + GET_ENTITIES_URL, schemaPattern);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, TABLE_INFOS_TYPE);
            }
        } catch (IOException e) {
            log.error("Error loading schema tables {}", schemaPattern, e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public List<ColumnInfo> getDatabaseColumns(String schema, String tableName) {
        try {
            log.debug("schema: {}, table: {}", schema, tableName);
            String uri = String.format(backendHostUrl + GET_ATTRIBUTES_URL, schema, tableName);
            log.debug("uri: {}", uri);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, DATABASE_COLUMNS_TYPE);
            }
        } catch (IOException e) {
            log.error("Error loading columns of table {} schema {}", tableName, schema, e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public QueryResult executeQuery(QueryRequest queryRequest) throws SQLException {
        try {
            HttpPost httpPost = new HttpPost(backendHostUrl + "/query/execute");
            return executeRequest(queryRequest, httpPost);
        } catch (Exception e) {
            String errMsg = String.format("Error executing query [%s]", queryRequest.getSql());
            log.error(errMsg, e);
            throw new SQLException(errMsg, e);
        }
    }

    private QueryResult executeRequest(QueryRequest queryRequest, HttpPost httpPost) throws IOException, DtmSqlException {
        String queryRequestJson = MAPPER.writeValueAsString(queryRequest);
        log.debug("Preparing the query [{}]", queryRequestJson);
        httpPost.setEntity(new StringEntity(queryRequestJson, ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = client.execute(httpPost)) {
            checkResponseStatus(response);
            InputStream content = response.getEntity().getContent();
            QueryResult result = MAPPER.readValue(content, QueryResult.class);
            log.info("Request received response {}", result);
            return result;
        }
    }

    @SneakyThrows
    private void checkResponseStatus(CloseableHttpResponse response) {
        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            try {
                String res = MAPPER.readValue(response.getEntity().getContent(), ResponseException.class)
                        .getExceptionMessage();
                log.error("The system returned an unsuccessful response: {}", res);
                throw new DtmSqlException(res != null && !res.isEmpty() ? res :
                        String.format("The system returned an unsuccessful response: %s", response.getStatusLine().getReasonPhrase()));
            } catch (DtmSqlException e) {
                throw e;
            } catch (Exception e) {
                log.error("The system returned an unsuccessful response: {}", response.getStatusLine().getReasonPhrase());
                throw new DtmSqlException(String.format("The system returned an unsuccessful response:%s", response.getStatusLine().getReasonPhrase()));
            }
        }
    }
}
