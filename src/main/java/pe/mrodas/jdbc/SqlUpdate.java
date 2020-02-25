package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.InOperator;
import pe.mrodas.jdbc.helper.Parameter;
import pe.mrodas.jdbc.helper.SqlDML;

public class SqlUpdate implements SqlDML {
    private final static String QUERY = "UPDATE <table> SET <fields> WHERE <filters>";
    private final List<String> fields = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private final LinkedHashMap<String, Object> fieldsMap = new LinkedHashMap<>();
    private final LinkedHashMap<String, Object> filtersMap = new LinkedHashMap<>();
    private final String table;
    private final boolean ignoreNullFields;
    private String error;

    public SqlUpdate(String table, boolean ignoreNullFields) {
        this.table = table;
        this.ignoreNullFields = ignoreNullFields;
    }

    public SqlUpdate(String table) {
        this(table, true);
    }

    @Override
    public SqlUpdate addField(String name, Object value) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Field name can't be null or empty!";
        else if (value == null) {
            if (ignoreNullFields) return this;
            fields.add(String.format("%s = NULL", name));
        } else {
            fields.add(this.getParameter(name));
            fieldsMap.put(name, value);
        }
        return this;
    }

    public SqlUpdate addFilter(String name, Object value) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Filter name can't be null or empty!";
        else if (value == null)
            error = String.format("Filter '%s' value can't be null!", name);
        else {
            filters.add(this.getParameter(name));
            filtersMap.put(name, value);
        }
        return this;
    }

    public <T> SqlUpdate addFilter(String name, List<T> values) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Filter name can't be null or empty!";
        else {
            InOperator<T> inOperator = new InOperator<>(this.sanitize(name), values);
            if (inOperator.isInvalid())
                error = String.format("Filter list '%s' can't be null or empty!", name);
            else {
                String fields = inOperator.getFields();
                filters.add(String.format("%s IN (%s)", name, fields));
                inOperator.getParameters().forEach(filtersMap::put);
            }
        }
        return this;
    }

    private String getParameter(String name) {
        return String.format("%s = :%s", name, this.sanitize(name));
    }

    private String sanitize(String name) {
        int endIndex = name.length() - 1;
        if (name.charAt(0) != name.charAt(endIndex)) return name;
        if (name.charAt(0) != '`') return name;
        String sanitized = name.substring(1, endIndex);
        if (sanitized.trim().isEmpty())
            error = String.format("Parameter '%s' can't be empty!", name);
        return sanitized;
    }

    public int execute() throws IOException, SQLException {
        return this.execute(null, null);
    }

    private int registerParameters(PreparedStatement statement, int initPos, LinkedHashMap<String, Object> fieldsMap) throws SQLException {
        Parameter.Position position = new Parameter.Position(initPos);
        try {
            for (Map.Entry<String, Object> entry : fieldsMap.entrySet()) {
                position.setName(entry.getKey());
                new Parameter<>(entry.getValue()).registerIN(statement, position.incrementAndGet());
            }
            return position.getPos();
        } catch (SQLException e) {
            String errorMsg = "Error setting '%s' parameter in statement! - %s";
            throw new SQLException(String.format(errorMsg, position.getName(), e.getMessage()), e);
        }
    }

    public int execute(Connection connection, Autoclose autoclose) throws IOException, SQLException {
        try {
            if (table == null) throw new IOException("Table name can't be null!");
            if (fields.isEmpty()) throw new IOException("Fields can't be empty!");
            if (filters.isEmpty()) throw new IOException("Filters can't be empty!");
            if (error != null) throw new IOException(error);
            String query = QUERY.replace("<table>", table)
                    .replace("<fields>", String.join(", ", fields))
                    .replace("<filters>", String.join(" AND ", filters));
            String preparedQuery = query.replaceAll(":\\w+", "?");
            PreparedStatement statement = (connection == null ? Connector.getConnection() : connection)
                    .prepareStatement(preparedQuery);
            int pos = this.registerParameters(statement, 0, this.fieldsMap);
            this.registerParameters(statement, pos - 1, this.filtersMap);
            statement.execute();
            return statement.getUpdateCount();
        } finally {
            this.close(connection, autoclose);
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
