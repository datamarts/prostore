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
package ru.datamart.prostore.jdbc.core;

import ru.datamart.prostore.jdbc.ext.DtmResultSet;

public class ResultSetWrapper {
    private final DtmResultSet resultSet;
    private final long updateCount;
    private ResultSetWrapper next;

    public ResultSetWrapper(DtmResultSet resultSet) {
        this.resultSet = resultSet;
        this.updateCount = resultSet.getRowsSize();
    }

    public DtmResultSet getResultSet() {
        return resultSet;
    }

    public ResultSetWrapper getNext() {
        return this.next;
    }

    public void append(ResultSetWrapper newResult) {
        ResultSetWrapper tail = this;
        while (tail.next != null) {
            tail = tail.next;
        }
        tail.next = newResult;
    }

    public long getUpdateCount() {
        return updateCount;
    }
}
