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

    public SqlDelete(String table) {
        this.table = table;
    }

    public SqlDelete addFilter(String name, Object value) {
        if (value == null) return this;
        this.filters.add(String.format("%s = :%s", name, name));
        this.filtersMap.put(name, value);
        return this;
    }

    public <T> SqlDelete addFilter(String name, List<T> values) {
        if (name == null || name.isEmpty()) return this;
        if (values == null || values.isEmpty()) return this;
        InOperator<T> inOperator = new InOperator<>(name, values);
        String fields = inOperator.getFields();
        this.filters.add(String.format("%s IN (%s)", name, fields));
        inOperator.getParameters().forEach(this.filtersMap::put);
        return this;
    }

    public int execute() throws IOException, SQLException {
        return this.execute(null, null);
    }

    public int execute(Connection connection, Autoclose autoclose) throws IOException, SQLException {
        if (table == null) throw new IOException("Table name can't be null!");
        if (filters.isEmpty()) throw new IOException("Filters can't be empty! Could delete all data!");
        SqlQuery<?> sqlQuery = (connection == null ? new SqlQuery<>()
                : new SqlQuery<>(connection, autoclose == null ? Autoclose.YES : autoclose));
        String preparedQuery = QUERY.replace("<table>", table)
                .replace("<filters>", String.join(" AND ", filters));
        sqlQuery.setSql(preparedQuery);
        this.filtersMap.forEach(sqlQuery::addParameter);
        return sqlQuery.execute();
    }

}
