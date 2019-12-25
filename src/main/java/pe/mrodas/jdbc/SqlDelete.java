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

public class SqlDelete {
    private final static String QUERY = "DELETE FROM <table> WHERE <filters>";
    private final List<String> filters = new ArrayList<>();
    private final Map<String, Object> filtersMap = new HashMap<>();
    private final String table;
    private String error;

    public SqlDelete(String table) {
        this.table = table;
    }

    public SqlDelete addFilter(String name, Object value) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Filter name can't be null or empty!";
        else if (value == null)
            error = String.format("Filter '%s' value can't be null!", name);
        else {
            this.filters.add(String.format("%s = :%s", name, name));
            this.filtersMap.put(name, value);
        }
        return this;
    }

    public <T> SqlDelete addFilter(String name, List<T> values) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Parameter name can't be null or empty!";
        else {
            InOperator<T> inOperator = new InOperator<>(name, values);
            if (inOperator.isInvalid())
                error = String.format("Parameter list '%s' can't be null or empty!", name);
            else {
                String filter = String.format("%s IN (%s)", name, inOperator.getFields());
                filters.add(filter);
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
        if (filters.isEmpty()) error = "Filters can't be empty!";
        if (error != null) throw new IOException(error);
        SqlQuery<?> sqlQuery = (connection == null ? new SqlQuery<>()
                : new SqlQuery<>(connection, autoclose == null ? Autoclose.YES : autoclose));
        String preparedQuery = QUERY.replace("<table>", table)
                .replace("<filters>", String.join(" AND ", filters));
        sqlQuery.setSql(preparedQuery);
        this.filtersMap.forEach(sqlQuery::addParameter);
        return sqlQuery.execute();
    }

}
