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
import ru.datamart.prostore.jdbc.ext.DtmStatement;
import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class DtmDriverTest {

    public static void main(String[] args) throws SQLException, InterruptedException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:prostore://%s/", host);
        AtomicLong t = new AtomicLong();
        int count = 3;
        long total = System.currentTimeMillis();
        for (int j = 0; j < 100; j++) {
            CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        long tt = System.currentTimeMillis();
                        extracted(host, user, schema, url, finalI);
                        t.addAndGet(System.currentTimeMillis() - tt);
                        latch.countDown();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }).start();
            }
            latch.await();
        }
        System.out.printf("avg %d; total %d%n", (t.get() / count), System.currentTimeMillis() - total);
    }

    @SneakyThrows
    private static void extracted(String host,
                                  String user,
                                  String schema,
                                  String url,
                                  int i) throws SQLException {
        try (BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url)) {
            DtmStatement stmnt = (DtmStatement) conn.createStatement();
            ResultSet resultSet = stmnt.executeQuery("select * from dtm_866.test_table where id = " + i);
            System.out.println(resultSet);
        }
    }
}
