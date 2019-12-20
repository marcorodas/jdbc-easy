package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import pe.mrodas.jdbc.helper.CursorIterator;
import pe.mrodas.jdbc.helper.Parameter;
import pe.mrodas.jdbc.helper.ThrowingBiFunction;

public class SqlFunction<T> {
    private final static String QUERY = "SELECT <function>(<parameters>) value";
    private final List<Object> parameters = new ArrayList<>();
    private final String functionName;
    private String error;

    public SqlFunction(String functionName) {
        this.functionName = functionName;
    }

    public SqlFunction<T> addParameter(Object parameter) {
        if (error != null) return this;
        if (parameter == null)
            error = String.format("Parameter #%s value can't be null!", parameters.size());
        else parameters.add(parameter);
        return this;
    }

    public T execute(ThrowingBiFunction<ResultSet, String, T> mapper) throws IOException, SQLException {
        return this.execute(null, mapper);
    }

    public T execute(Connection connection, ThrowingBiFunction<ResultSet, String, T> mapper) throws IOException, SQLException {
        if (functionName == null) throw new IOException("Function name can't be null!");
        if (error != null) throw new IOException(error);
        int numParameters = parameters.size();
        List<String> params = Collections.nCopies(numParameters, "?");
        String preparedQuery = QUERY.replace("<function>", functionName)
                .replace("<parameters>", String.join(", ", params));
        Connection conn = connection == null ? Connector.getConnection() : connection;
        PreparedStatement statement = conn.prepareStatement(preparedQuery);
        CursorIterator iterator = new CursorIterator(numParameters);
        try {
            for (Integer pos : iterator)
                new Parameter<>(parameters.get(pos))
                        .registerIN(statement, pos + 1);
        } catch (SQLException e) {
            String name = String.format("#%s", iterator.getPos() + 1);
            String msg = String.format("Error setting '%s' parameter in statement! - ", name);
            throw new SQLException(msg + e.getMessage(), e);
        }
        statement.execute();
        ResultSet rs = statement.getResultSet();
        if (rs.next()) try {
            return mapper.apply(rs, "value");
        } catch (Exception e) {
            throw new IOException("Mapping Error: " + e.getMessage(), e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
