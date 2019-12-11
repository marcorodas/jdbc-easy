package pe.mrodas.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.CursorIterator;
import pe.mrodas.jdbc.helper.GeneratedKeys;
import pe.mrodas.jdbc.helper.InOperator;
import pe.mrodas.jdbc.helper.SqlStatement;

public class SqlQuery<T> extends SqlStatement<T> {

    private GeneratedKeys generatedKeys;
    private String query, originalQuery;
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
        if (this.error != null) return this;
        if (name == null || name.trim().isEmpty())
            this.error = "Parameter name can't be null or empty!";
        else if (value == null)
            this.error = String.format("Parameter '%s' value can't be null!", name);
        else this.parameters.put(name, value);
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
        if (this.error != null) return this;
        if (name == null || name.trim().isEmpty())
            this.error = "Parameter name can't be null or empty!";
        else {
            InOperator<S> inOperator = new InOperator<>(name, values);
            if (inOperator.isInvalid())
                this.error = String.format("Parameter '%s' value can't be null or empty!", name);
            else {
                if (!this.inReplacement.containsKey(name)) {
                    this.inReplacement.put(name, inOperator.getFields());
                    inOperator.getParameters().forEach(this.parameters::put);
                }
            }
        }
        return this;
    }

    private void prepareQuery() {
        if (!this.parametersInQuery.isEmpty() || this.error != null) return;
        if (this.query == null || this.query.isEmpty()) {
            this.error = "Query can't be null or empty!";
            return;
        }
        this.inReplacement.forEach((name, fields) -> this.query = this.query.replace(name, fields));
        this.originalQuery = this.query;
        Matcher matcher = Pattern.compile(":\\w+").matcher(this.query);
        while (matcher.find()) {
            String paramNameInQuery = matcher.group().substring(1);
            if (this.parameters.containsKey(paramNameInQuery))
                this.parametersInQuery.add(paramNameInQuery);
            else {
                this.error = String.format("Missing parameter '%s'!", paramNameInQuery);
                break;
            }
        }
        if (this.error != null) return;
        this.parameters.keySet().forEach(name -> this.query = this.query.replace(":" + name, "?"));
    }

    private PreparedStatement getPreparedStatement() throws SQLException, IOException {
        Connection connection = super.getConnection();
        return generatedKeys == GeneratedKeys.RETURN
                ? connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
                : connection.prepareStatement(query);
    }

    @Override
    protected SQLException buildCallableException(SQLException e) {
        String msj = String.format("%s Query:(%s)", e.getMessage(), this.originalQuery);
        return new SQLException(msj, e);
    }

    @Override
    protected PreparedStatement executeStatement() throws SQLException, IOException {
        this.prepareQuery();
        if (this.error != null) throw new IOException(this.error);
        PreparedStatement statement = this.getPreparedStatement();
        int size = parametersInQuery.size();
        for (Integer pos : new CursorIterator(size)) {
            String name = parametersInQuery.get(pos);
            Object value = parameters.get(name);
            SqlQuery.tryRegisterParameter(statement, pos, name, value);
        }
        statement.execute();
        return statement;
    }

    static void tryRegisterParameter(PreparedStatement statement, Integer pos, String name, Object value) throws SQLException {
        try {
            SqlQuery.registerParameter(statement, pos + 1, value);
        } catch (Exception e) {
            String error = String.format("Error setting '%s' parameter in statement! - ", name);
            throw new SQLException(error + e.getMessage(), e);
        }
    }

    static void registerParameter(PreparedStatement statement, int index, Object value) throws SQLException {
        Class<?> objClass = value.getClass();
        if (objClass.isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType != null && componentType.isPrimitive())
                if (byte.class.isAssignableFrom(componentType))
                    statement.setBytes(index, (byte[]) value);
        } else if (objClass == Integer.class) statement.setInt(index, (Integer) value);
        else if (objClass == String.class) statement.setString(index, (String) value);
        else if (objClass == Boolean.class) statement.setBoolean(index, (Boolean) value);
        else if (objClass == Double.class) statement.setDouble(index, (Double) value);
        else if (objClass == Float.class) statement.setFloat(index, (Float) value);
        else if (value instanceof InputStream) statement.setBlob(index, (InputStream) value);
        else if (objClass == Date.class) {
            long time = ((Date) value).getTime();
            statement.setTimestamp(index, new Timestamp(time));
        } else if (objClass == Time.class) statement.setTime(index, (Time) value);
        else if (objClass == Timestamp.class) statement.setTimestamp(index, (Timestamp) value);
        else if (objClass == LocalDate.class)
            statement.setDate(index, java.sql.Date.valueOf((LocalDate) value));
        else if (objClass == LocalTime.class) statement.setTime(index, Time.valueOf((LocalTime) value));
        else if (objClass == LocalDateTime.class)
            statement.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
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
            if (this.generatedKeys != GeneratedKeys.RETURN)
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