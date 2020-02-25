package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.GeneratedKeys;
import pe.mrodas.jdbc.helper.InOperator;
import pe.mrodas.jdbc.helper.Parameter;
import pe.mrodas.jdbc.helper.SqlStatement;

public class SqlQuery<T> extends SqlStatement<T> {

    private GeneratedKeys generatedKeys;
    private String query, preparedQuery;
    private List<String> parametersInQuery = new ArrayList<>();
    private final HashMap<String, Object> parameters = new HashMap<>();
    private final HashMap<String, String> inReplacement = new HashMap<>();
    private String error;

    public SqlQuery(Connection connection, Autoclose autoclose) {
        super(connection, autoclose);
    }

    public SqlQuery() {
        super();
    }

    public SqlQuery<T> setSql(String sql, GeneratedKeys generatedKeys) {
        this.query = sql;
        this.generatedKeys = generatedKeys;
        return this;
    }

    public SqlQuery<T> setSql(String[] sql, GeneratedKeys generatedKeys) {
        return this.setSql(String.join(" ", sql), generatedKeys);
    }

    public SqlQuery<T> setSql(String sql) {
        return this.setSql(sql, GeneratedKeys.NO_RETURN);
    }

    public SqlQuery<T> setSql(String[] sql) {
        return this.setSql(String.join(" ", sql), GeneratedKeys.NO_RETURN);
    }

    /**
     * Agrega un nuevo parámetro definido con la sintaxis ":parameter"
     *
     * @param name  Nombre del parámetro. Sin ":" (key)
     * @param value Valor del parámetro (value)
     * @return El mismo objeto SqlQuery
     */
    public SqlQuery<T> addParameter(String name, Object value) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Parameter name can't be null or empty!";
        else if (value == null)
            error = String.format("Parameter '%s' value can't be null!", name);
        else parameters.put(name, value);
        return this;
    }

    /**
     * Agrega una serie de parámetros definidos con la sintaxis ":parameterList"
     * que luego serán reemplazados por los correlativos ":parameterList0, :parameterList1, ..."
     * Destinado a usarse en una sentencia IN (:parameterList)
     *
     * @param name   Nombre de la serie de parámetros. Sin ":" (key)
     * @param values Lista de valores de los parámetros (value)
     * @return El mismo objeto SqlQuery
     */
    public <S> SqlQuery<T> addParameter(String name, List<S> values) {
        if (error != null) return this;
        if (name == null || name.trim().isEmpty())
            error = "Parameter name can't be null or empty!";
        else {
            InOperator<S> inOperator = new InOperator<>(name, values);
            if (inOperator.isInvalid())
                error = String.format("Parameter list '%s' can't be null or empty!", name);
            else {
                String key = ":".concat(name);
                if (!inReplacement.containsKey(key)) {
                    inReplacement.put(key, inOperator.getFields());
                    inOperator.getParameters().forEach(parameters::put);
                }
            }
        }
        return this;
    }

    @Override
    protected SQLException buildCallableException(SQLException e) {
        String msj = String.format("%s Query:(%s)", e.getMessage(), query);
        return new SQLException(msj, e);
    }

    private String getPreparedQuery() throws IOException {
        if (query == null || query.trim().isEmpty())
            throw new IOException("Query can't be null or empty!");
        if (error != null) throw new IOException(error);
        this.inReplacement.forEach((key, fields) -> query = query.replace(key, fields));
        Matcher matcher = Pattern.compile(":\\w+").matcher(query);
        while (matcher.find()) {
            String paramNameInQuery = matcher.group().substring(1);
            if (parameters.containsKey(paramNameInQuery))
                parametersInQuery.add(paramNameInQuery);
            else throw new IOException(String.format("Missing parameter '%s'!", paramNameInQuery));
        }
        return query.replaceAll(":\\w+", "?");
    }

    @Override
    protected PreparedStatement executeStatement() throws SQLException, IOException {
        preparedQuery = preparedQuery == null ? this.getPreparedQuery() : preparedQuery;
        Connection connection = super.getConnection();
        PreparedStatement statement = generatedKeys == GeneratedKeys.RETURN
                ? connection.prepareStatement(preparedQuery, Statement.RETURN_GENERATED_KEYS)
                : connection.prepareStatement(preparedQuery);
        Parameter.Position position = new Parameter.Position(0);
        try {
            for (String name : parametersInQuery) {
                position.setName(name);
                new Parameter<>(parameters.get(name)).registerIN(statement, position.incrementAndGet());
            }
        } catch (SQLException e) {
            String errorMsg = "Error setting '%s' parameter in statement! - %s";
            throw new SQLException(String.format(errorMsg, position.getName(), e.getMessage()), e);
        }
        statement.execute();
        return statement;
    }

    /**
     * Ejectua el query. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     * <br><br>
     * Se puede instanciar la clase como: <br>
     * <code>SqlQuery query = new SqlQuery();</code>
     *
     * @return rowCount/ID <b>rowCount</b><i>(default)</i>: Update count o -1 si
     * el resultado es un ResultSet o no hay más resultados. <br>
     * <b>ID<i>(Si GeneratedKeys.RETURN fue seleccionado)</i></b>: Primer ID
     * autogenerado a partir de un INSERT.
     */
    public int execute() throws IOException, SQLException {
        try {
            if (generatedKeys != GeneratedKeys.RETURN)
                return this.executeStatement().getUpdateCount();
            ResultSet rs = this.executeStatement().getGeneratedKeys();
            if (rs.next()) {
                int autoGeneratedKey = rs.getInt(1);
                if (autoGeneratedKey > 0) return autoGeneratedKey;
            }
            throw new SQLException("Error getting autogenerated key!");
        } finally {
            super.close();
        }
    }

}