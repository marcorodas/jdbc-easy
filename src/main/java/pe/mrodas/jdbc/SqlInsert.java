package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
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
import pe.mrodas.jdbc.helper.Parameter;
import pe.mrodas.jdbc.helper.SqlDML;
import pe.mrodas.jdbc.helper.TableIterator;

public class SqlInsert implements SqlDML {

    private final static String QUERY = "INSERT INTO <table> (<fields>) VALUES (<values>)";
    private final Map<String, List<Parameter<?>>> valueListMap = new HashMap<>();
    private final String table;
    private final Consumer<Integer> setterId;
    private String error;
    private int totalRows;

    public SqlInsert(String table) {
        this(table, null);
    }

    public SqlInsert(String table, Consumer<Integer> setterId) {
        this.table = table.replace(" ", "");
        this.setterId = setterId;
    }

    @Override
    public SqlInsert addField(String name, Object value) {
        return this.addField(name, new Parameter<>(value), false);
    }

    public SqlInsert addField(String name, Object value, JDBCType type) {
        return this.addField(name, new Parameter<>(value, type), true);
    }

    public <P> SqlInsert addField(String name, P value, Class<P> objClass) {
        return this.addField(name, new Parameter<>(value, objClass), true);
    }

    private SqlInsert addField(String name, Parameter<?> parameter, boolean allowNull) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Field name can't be null or empty!";
        else if (!allowNull && parameter.valueIsNull()) {
            error = String.format("'%s' value is null but lacks JdbcType or Class<> definition in SqlInsert.addFieldMethod!", name);
        } else {
            if (!valueListMap.containsKey(name)) valueListMap.put(name, new ArrayList<>());
            valueListMap.get(name).add(parameter);
        }
        return this;
    }

    private String getPreparedQuery(List<String> fieldNames) {
        List<String> questionMarks = Collections.nCopies(fieldNames.size(), "?");
        return QUERY.replace("<table>", table)
                .replace("<fields>", String.join(", ", fieldNames))
                .replace("<values>", String.join(", ", questionMarks));
    }

    private PreparedStatement getPreparedStatement(Connection conn, String preparedQuery) throws SQLException {
        return this.setterId == null
                ? conn.prepareStatement(preparedQuery)
                : conn.prepareStatement(preparedQuery, Statement.RETURN_GENERATED_KEYS);
    }

    public void executeStatement(PreparedStatement statement, List<String> fieldNames) throws SQLException {
        TableIterator iterator = new TableIterator(totalRows, fieldNames.size());
        try {
            for (Integer row : iterator.getRowIterator()) {
                for (Integer col : iterator.getColIterator().reset()) {
                    String name = fieldNames.get(col);
                    Parameter<?> parameter = valueListMap.get(name).get(row);
                    if (parameter.valueIsNull())
                        statement.setNull(col + 1, parameter.getSqlType());
                    else parameter.registerIN(statement, col + 1);
                }
                if (totalRows > 1) statement.addBatch();
            }
        } catch (SQLException e) {
            if (!iterator.getColIterator().hasNext()) throw e;
            String name = fieldNames.get(iterator.getPosCol());
            String errorMsg = "Insert into %s: Error setting '%s' parameter (row=%s) in statement! - %s";
            String error = String.format(errorMsg, this.table, name, iterator.getPosRow(), e.getMessage());
            throw new SQLException(error, e);
        }
        if (totalRows > 1) statement.executeBatch();
        else statement.execute();
    }

    public int execute() throws IOException, SQLException {
        return this.execute(null, null);
    }

    private String checkNumRows() {
        if (error == null) for (Map.Entry<String, List<Parameter<?>>> entry : valueListMap.entrySet()) {
            int numRowsByField = entry.getValue().size();
            if (totalRows == 0) totalRows = numRowsByField;
            if (totalRows != numRowsByField) {
                String fieldName = entry.getKey();
                String msg = totalRows > numRowsByField ? "less" : "more";
                return String.format("Fields Error: %s has %s rows than other fields!", fieldName, msg);
            }
        }
        return error;
    }

    public int execute(Connection connection, Autoclose autoclose) throws IOException, SQLException {
        if (table == null) throw new IOException("Table name can't be null!");
        if (valueListMap.isEmpty()) error = "Fields can't be empty!";
        error = this.checkNumRows();
        if (error != null) throw new IOException(error);
        Connection conn = connection == null ? Connector.getConnection() : connection;
        List<String> fieldNames = new ArrayList<>(valueListMap.keySet());
        String preparedQuery = this.getPreparedQuery(fieldNames);
        PreparedStatement statement = this.getPreparedStatement(conn, preparedQuery);
        this.executeStatement(statement, fieldNames);
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
            this.close(conn, autoclose == null ? Autoclose.YES : autoclose);
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
