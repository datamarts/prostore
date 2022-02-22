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
package ru.datamart.prostore.jdbc.ext;

import ru.datamart.prostore.jdbc.core.*;
import ru.datamart.prostore.jdbc.util.DtmSqlException;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DtmStatement implements BaseStatement {
    /**
     * DTM connection
     */
    protected final BaseConnection connection;
    /**
     * ResultSet type (ResultSet.TYPE_xxx)
     */
    protected final int resultSetScrollType;
    /**
     * Is result set updatable (ResultSet.CONCUR_xxx)
     */
    protected final int concurrency;
    /**
     * Max rows count
     */
    protected long maxRows;
    /**
     * Return rows count
     */
    protected int fetchSize;
    /**
     * Result set wrapper
     */
    protected ResultSetWrapper result;
    /**
     * Is escape substitution enabled
     */
    protected boolean escapeProcessingEnabled = true;
    protected int maxFieldSize = 0;
    protected ArrayList<Query> batchStatements = null;
    protected SQLWarning warnings = null;
    protected int fetchDirection = ResultSet.FETCH_FORWARD;
    /**
     * Timeout (in seconds) for a query.
     */
    protected int timeout = 0;
    /**
     * Is connection closed
     */
    private boolean isClosed;
    private boolean poolable;

    public DtmStatement(BaseConnection c, int rsType, int rsConcurrency) {
        this.connection = c;
        this.resultSetScrollType = rsType;
        this.concurrency = rsConcurrency;
        this.isClosed = false;
        this.result = null;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (executeInternal(sql, null, fetchSize, Statement.NO_GENERATED_KEYS)) {
            return this.getSingleResultSet();
        }
        return DtmResultSet.createEmptyResultSet();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.debug("execute: {}", sql);
        return executeInternal(sql, null, fetchSize, Statement.NO_GENERATED_KEYS);
    }

    protected boolean execute(String sql, QueryParameters parameters) throws SQLException {
        return executeInternal(sql, parameters, fetchSize, Statement.NO_GENERATED_KEYS);
    }

    protected void prepareQuery(String sql) throws SQLException {
        List<Query> queries = this.connection.getQueryExecutor().createQuery(sql);
        if (queries.size() > 1) {
            throw new DtmSqlException("Multiple prepared statement query doesn't support");
        }
    }

    private boolean executeInternal(String sql, QueryParameters parameters, int fetchSize, int noGeneratedKeys) throws SQLException {
        log.debug("executeInternal: {}", sql);
        List<Query> queries = this.connection.getQueryExecutor().createQuery(sql);
        DtmResultHandler resultHandler = new DtmResultHandler();
        if (queries.size() == 1) {
            this.connection.getQueryExecutor().execute(queries.get(0), parameters, resultHandler);
        } else {
            this.connection.getQueryExecutor().execute(queries, null, resultHandler);
        }
        if (resultHandler.getException() == null) {
            this.result = resultHandler.getResult();
            return result != null;
        } else {
            throw new SQLException(resultHandler.getException());
        }
    }

    protected ResultSet getSingleResultSet() throws SQLException {
        synchronized (this) {
            this.checkClosed();
            ResultSetWrapper result = this.result;
            if (result.getNext() != null) {
                throw new DtmSqlException("Multiple ResultSets were returned by the query.");
            } else {
                return result.getResultSet();
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        log.debug("executeUpdate: {}", sql);
        execute(sql);
        return getUpdateCount();
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("The maximum field size must be a value greater than or equal to 0.");
        }
        maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        escapeProcessingEnabled = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return timeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("Query timeout must be a value greater than or equals to 0.");
        }
        timeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        this.checkClosed();
        return this.result == null ? null : this.result.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        if (result == null || result.getResultSet() != null) {
            return -1;
        }

        long count = result.getUpdateCount();
        return count > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : (int) count;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                fetchDirection = direction;
                break;
            default:
                throw new SQLException("Invalid fetch direction constant: {0}.");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0 && rows != Integer.MIN_VALUE) {
            throw new SQLException(String.format("Incorrect %d value for block size", rows));
        } else if (rows == Integer.MIN_VALUE) {
            //for compatibility Integer.MIN_VALUE is transform to 0 => streaming
            this.fetchSize = 1;
            return;
        }
        this.fetchSize = rows;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return concurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetScrollType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        ArrayList<Query> batchStatements = this.batchStatements;
        if (batchStatements == null) {
            this.batchStatements = batchStatements = new ArrayList<>();
        }

        List<Query> query = connection.getQueryExecutor().createQuery(sql);
        batchStatements.addAll(query);
    }

    @Override
    public void clearBatch() throws SQLException {
        if (batchStatements != null) {
            batchStatements.clear();
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        List<Long> updateCounts = new ArrayList<>();

        if (batchStatements == null || batchStatements.isEmpty()) {
            return new int[0];
        }

        DtmResultHandler resultHandler = new DtmResultHandler();
        connection.getQueryExecutor().execute(batchStatements, null, resultHandler);

        ResultSetWrapper currentResult = resultHandler.getResult();
        while (currentResult != null) {
            updateCounts.add(currentResult.getUpdateCount());
            currentResult = currentResult.getNext();
        }

        return updateCounts.stream()
            .mapToInt(count -> count > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : count.intValue())
            .toArray();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (current == KEEP_CURRENT_RESULT || current == CLOSE_ALL_RESULTS) {
            throw new SQLFeatureNotSupportedException();
        }
        this.checkClosed();
        if (current == Statement.CLOSE_CURRENT_RESULT && result != null
            && result.getResultSet() != null) {
            result.getResultSet().close();
        }

        if (result != null) {
            result = result.getNext();
        }
        return result != null && result.getResultSet() != null;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return executeInternal(sql, null, fetchSize, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return executeInternal(sql, null, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return executeInternal(sql, null, fetchSize, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    protected void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw new DtmSqlException("This statement has been closed.");
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        this.poolable = poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        close();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public ResultSet createDriverResultSet(Field[] fields, List<Tuple> tuples) {
        return createResultSet(fields, tuples);
    }

    private DtmResultSet createResultSet(Field[] fields, List<Tuple> tuples) {
        return new DtmResultSet(this.connection, this, fields, tuples);
    }

    public class DtmResultHandler extends ResultHandlerBase {
        private ResultSetWrapper results;
        private ResultSetWrapper lastResult;

        ResultSetWrapper getResult() {
            return this.results;
        }

        private void append(ResultSetWrapper newResult) {
            if (this.results == null) {
                this.lastResult = this.results = newResult;
            } else {
                this.lastResult.append(newResult);
            }
        }

        @Override
        public void handleResultRows(Query query, Field[] fields, List<Tuple> tuples) {
            try {
                ResultSet rs = createResultSet(fields, tuples);
                this.append(new ResultSetWrapper((DtmResultSet) rs));
            } catch (Exception e) {
                this.handleError(new SQLException(e));
            }
        }
    }
}
