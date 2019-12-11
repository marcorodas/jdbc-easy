package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.CursorIterator;
import pe.mrodas.jdbc.helper.SqlDML;
import pe.mrodas.jdbc.helper.TableIterator;

public class SqlInsert implements SqlDML {

    private final static String QUERY = "INSERT INTO <table> (<fields>) VALUES (<values>)";
    private final Map<String, List<Object>> valueListMap = new HashMap<>();
    private final String table;
    private final Consumer<Integer> setterId;

    public SqlInsert(String table) {
        this(table, null);
    }

    public SqlInsert(String table, Consumer<Integer> setterId) {
        this.table = table.replace(" ", "");
        this.setterId = setterId;
    }

    @Override
    public SqlInsert addField(String name, Object value) {
        if (name == null || value == null) return this;
        if (!valueListMap.containsKey(name)) valueListMap.put(name, new ArrayList<>());
        valueListMap.get(name).add(value);
        return this;
    }

    private int checkNumRows(List<String> fieldNames) throws IOException {
        List<Integer> rows = new ArrayList<>(1);
        for (String name : fieldNames) {
            int listSize = valueListMap.get(name).size();
            if (rows.isEmpty()) rows.add(listSize);
            else if (rows.get(0) != listSize) {
                String msg = listSize > rows.get(0) ? "more" : "less";
                throw new IOException(String.format("Fields Error: %s has %s rows than other fields!", name, msg));
            }
        }
        return valueListMap.get(fieldNames.get(0)).size();
    }

    private String getPreparedQuery(List<String> fieldNames) {
        List<String> values = Collections.nCopies(valueListMap.keySet().size(), "?");
        return QUERY.replace("<table>", table)
                .replace("<fields>", String.join(", ", fieldNames))
                .replace("<values>", String.join(", ", values));
    }

    private PreparedStatement getPreparedStatement(Connection conn, String preparedQuery) throws SQLException {
        return this.setterId == null
                ? conn.prepareStatement(preparedQuery)
                : conn.prepareStatement(preparedQuery, Statement.RETURN_GENERATED_KEYS);
    }

    private void tryRegisterParameter(PreparedStatement statement, String name, TableIterator tableIterator) throws SQLException {
        int row = tableIterator.getPosRow();
        int col = tableIterator.getPosCol();
        Object value = valueListMap.get(name).get(row);
        try {
            SqlQuery.registerParameter(statement, col + 1, value);
        } catch (Exception e) {
            String error = String.format("Insert into %s: Error setting '%s' parameter (row=%s) in statement! - ", this.table, name, row) + e.getMessage();
            throw new SQLException(error, e);
        }
    }

    public void executeStatement(PreparedStatement statement, List<String> fieldNames, int totalRows) throws SQLException {
        int totalCols = fieldNames.size();
        if (totalRows == 1) {
            for (Integer col : new CursorIterator(totalCols)) {
                String name = fieldNames.get(col);
                Object value = valueListMap.get(name).get(0);
                SqlQuery.tryRegisterParameter(statement, col, name, value);
            }
            statement.execute();
            return;
        }
        TableIterator tableIterator = new TableIterator(totalRows, totalCols);
        for (Integer row : tableIterator.getRowIterator()) {
            for (Integer col : tableIterator.getColIterator()) {
                String name = fieldNames.get(col);
                this.tryRegisterParameter(statement, name, tableIterator);
            }
            statement.addBatch();
        }
        statement.executeBatch();
    }

    public int execute() throws IOException, SQLException {
        return this.execute(null, Autoclose.YES);
    }

    public int execute(Connection connection, Autoclose autoclose) throws IOException, SQLException {
        if (table == null) throw new IOException("Table name can't be null!");
        if (valueListMap.isEmpty()) throw new IOException("Fields can't be empty!");
        List<String> fieldNames = new ArrayList<>(valueListMap.keySet());
        int totalRows = this.checkNumRows(fieldNames);
        Connection conn = connection == null ? Connector.getConnection() : connection;
        String preparedQuery = this.getPreparedQuery(fieldNames);
        PreparedStatement statement = this.getPreparedStatement(conn, preparedQuery);
        this.executeStatement(statement, fieldNames, totalRows);
        try {
            if (setterId == null) return statement.getUpdateCount();
            ResultSet rs = statement.getGeneratedKeys();
            if (rs.next()) {
                int autoGeneratedKey = rs.getInt(1);
                setterId.accept(autoGeneratedKey);
                if (autoGeneratedKey > 0) return autoGeneratedKey;
            }
            throw new SQLException("Error getting autogenerated key!");
        } finally {
            this.close(conn, autoclose);
        }
    }

    private void close(Connection conn, Autoclose autoclose) {
        if (autoclose == Autoclose.YES) try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
