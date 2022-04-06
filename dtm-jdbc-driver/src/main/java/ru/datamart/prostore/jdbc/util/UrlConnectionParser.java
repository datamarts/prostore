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
package ru.datamart.prostore.jdbc.util;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Properties;

import static ru.datamart.prostore.jdbc.util.DriverConstants.*;

@Slf4j
public class UrlConnectionParser {

    private UrlConnectionParser() {
    }

    public static Properties parseURL(String url, Properties info) {
        Properties urlProperties = new Properties(info);
        if (!url.startsWith(CONNECT_URL_PREFIX)) {
            return null;
        }
        String urlServer = "http://" + url.replaceFirst(CONNECT_URL_PREFIX, "");
        URI uri = URI.create(urlServer);

        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            log.error("JDBC URL must contain the host and port db: {}", url);
            return null;
        }
        int port = uri.getPort();
        if (port < 1 || port > 65535) {
            log.error("JDBC URL port: {} not valid (1:65535) ", port);
            return null;
        }
        host += ":" + uri.getPort();

        urlProperties.setProperty(SCHEMA_PROPERTY, getSchema(uri));
        urlProperties.setProperty(HOST_PROPERTY, host);

        String query = uri.getQuery();
        if (query != null) {
            String[] args = query.split("&");
            for (String token : args) {
                if (token.isEmpty()) {
                    continue;
                }
                int pos = token.indexOf('=');
                if (pos == - 1) {
                    urlProperties.setProperty(token, "");
                } else {
                    urlProperties.setProperty(token.substring(0, pos), token.substring(pos + 1));
                }
            }
        }
        return urlProperties;
    }

    private static String getSchema(URI uri) {
        String path = uri.getPath();
        return path == null || path.isEmpty() ? "" : path.substring(1);
    }
}
