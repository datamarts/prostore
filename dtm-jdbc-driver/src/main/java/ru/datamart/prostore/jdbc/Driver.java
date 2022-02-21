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
package ru.datamart.prostore.jdbc;

import ru.datamart.prostore.jdbc.ext.DtmConnectionImpl;
import ru.datamart.prostore.jdbc.util.DriverInfo;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static ru.datamart.prostore.jdbc.util.DriverConstants.*;
import static ru.datamart.prostore.jdbc.util.UrlConnectionParser.parseURL;

@Slf4j
public class Driver implements java.sql.Driver {

    private static final Logger PARENT_LOGGER = LoggerFactory.getLogger("ru.datamart.prostore.jdbc");

    static {
        try {
            DriverManager.registerDriver(new Driver());
            log.info("Driver registered");
        } catch (SQLException e) {
            log.error("Error registering JDBC driver", e.getCause());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        if (!url.startsWith(CONNECT_URL_PREFIX)) {
            return null;
        }
        Properties props = parseURL(url, info);
        if (props == null) {
            throw new SQLException("Error parsing URL: " + url);
        }
        return makeConnection(url, props);
    }

    private static Connection makeConnection(String url, Properties info) throws SQLException {
        return new DtmConnectionImpl(dbHost(info), user(info), schema(info), info, url);
    }

    private static String schema(Properties info) {
        return info.getProperty(SCHEMA_PROPERTY, "");
    }

    private static String user(Properties info) {
        return info.getProperty(USER_PROPERTY, "");
    }

    private static String dbHost(Properties info) {
        return info.getProperty(HOST_PROPERTY, "");
    }

    @Override
    public boolean acceptsURL(String url) {
        return parseURL(url, null) != null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DriverInfo.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DriverInfo.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return (java.util.logging.Logger) PARENT_LOGGER;
    }
}
