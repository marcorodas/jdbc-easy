package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.InOperator;
import pe.mrodas.jdbc.helper.SqlDML;

public class SqlUpdate implements SqlDML {
    private final static String QUERY = "UPDATE <table> SET <fields> WHERE <filters>";
    private final List<String> fields = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private final Map<String, Object> fieldsMap = new HashMap<>();
    private final Map<String, Object> filtersMap = new HashMap<>();
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
            fields.add(String.format("%s = :%s", name, name));
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
            filters.add(String.format("%s = :%s", name, name));
            filtersMap.put(name, value);
        }
        return this;
    }

    public <T> SqlUpdate addFilter(String name, List<T> values) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Filter name can't be null or empty!";
        else {
            InOperator<T> inOperator = new InOperator<>(name, values);
            if (inOperator.isInvalid())
                error = String.format("Filter list '%s' can't be null or empty!", name);
            else {
                filters.add(String.format("%s IN (%s)", name, inOperator.getFields()));
                inOperator.getParameters().forEach(filtersMap::put);
            }
        }
        return this;
    }

    public int execute() throws IOException, SQLException {
        return this.execute(null, null);
    }

    public int execute(Connection connection, Autoclose autoclose) throws IOException, SQLException {
        if (table == null) throw new IOException("Table name can't be null!");
        if (fields.isEmpty()) throw new IOException("Fields can't be empty!");
        if (filters.isEmpty()) throw new IOException("Filters can't be empty!");
        if (error != null) throw new IOException(error);
        SqlQuery<?> sqlQuery = (connection == null ? new SqlQuery<>()
                : new SqlQuery<>(connection, autoclose == null ? Autoclose.YES : autoclose));
        String preparedQuery = QUERY.replace("<table>", table)
                .replace("<fields>", String.join(", ", fields))
                .replace("<filters>", String.join(" AND ", filters));
        sqlQuery.setSql(preparedQuery);
        this.fieldsMap.forEach(sqlQuery::addParameter);
        this.filtersMap.forEach(sqlQuery::addParameter);
        return sqlQuery.execute();
    }

}
