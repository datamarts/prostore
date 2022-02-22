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
package ru.datamart.prostore.jdbc;

import ru.datamart.prostore.jdbc.core.BaseConnection;
import ru.datamart.prostore.jdbc.ext.DtmConnectionImpl;
import ru.datamart.prostore.jdbc.ext.DtmPreparedStatement;
import ru.datamart.prostore.jdbc.ext.DtmStatement;

import java.sql.*;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:prostore://%s/", host);
        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
//        testPrepareStmnt(conn);
//        DtmStatement stmnt = (DtmStatement) conn.createStatement();
//        DatabaseMetaData metaData = conn.getMetaData();
//        ResultSet resultSet = stmnt.executeQuery("use dtm_1106");
        //ResultSet resultSet = metaData.getSchemas();
        //ResultSet resultSet = metaData.getColumns("dtm_1012", "", "accounts_all", null);
//        conn.prepareStatement("begin delta");
        final ResultSet resultSet = testPrepareStmnt(conn);
        //final ResultSet resultSet = testStmnt(conn);
        System.out.println(resultSet);
    }

    private static ResultSet testStmnt(BaseConnection conn) throws SQLException {
        String sql = "select t1.* from dtm_1046.accounts t1 datasource_type='ADB'";
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        return stmnt.executeQuery(sql);
    }

    private static ResultSet testPrepareStmnt(BaseConnection conn) throws SQLException {
        final String sql = "select * from testdb828.accounts WHERE id = ?\n" +
            "  AND double_col = ?\n" +
            "  AND float_col = ?\n" +
            "  AND varchar_col = ?\n" +
            "  AND boolean_col = ?\n" +
            "  AND int_col = ?\n" +
            "  AND bigint_col = ?\n" +
            "  AND date_col = ?\n" +
            "  AND timestamp_col = ?\n" +
            "  AND time_col = ?\n" +
            "  AND uuid_col = ?\n" +
            "  AND char_col = ?\n" +
            "  AND int32_col = ?\n" +
            "  AND link_col = ?  " +
            " datasource_type = 'adg'";

        DtmPreparedStatement stmnt = (DtmPreparedStatement) conn.prepareStatement(sql);
        stmnt.setObject(1, 1);
        stmnt.setObject(2, 1);
        stmnt.setObject(3, 1);
        stmnt.setObject(4, "TEST");
        stmnt.setObject(5, true);
        stmnt.setObject(6, 1);
        stmnt.setObject(7, 100000);
        stmnt.setObject(8, Date.valueOf("2020-11-28"));
        stmnt.setObject(9, Timestamp.valueOf("2020-11-17 21:11:12.000000"));
        stmnt.setObject(10, Time.valueOf("00:01:40"));
        stmnt.setObject(11, "d92beee8-749f-4539-aa15-3d2941dbb0f1");
        stmnt.setObject(12, 'x');
        stmnt.setObject(13, 32);
        stmnt.setObject(14, "https://google.com");
        ParameterMetaData parameterMetaData = stmnt.getParameterMetaData();
        System.out.println(parameterMetaData);
        return stmnt.executeQuery();
    }
}
