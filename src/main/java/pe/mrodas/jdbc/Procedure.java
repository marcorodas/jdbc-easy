package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import pe.mrodas.jdbc.helper.Autoclose;
import pe.mrodas.jdbc.helper.CursorIterator;
import pe.mrodas.jdbc.helper.Parameter;
import pe.mrodas.jdbc.helper.SqlStatement;
import pe.mrodas.jdbc.helper.SqlThrowingBiConsumer;
import pe.mrodas.jdbc.helper.ThrowingBiConsumer;
import pe.mrodas.jdbc.helper.ThrowingBiFunction;

public class Procedure<T> extends SqlStatement<T> {

    private String procedureName, preparedCall;
    private final HashMap<String, Parameter<?>> parametersIn = new HashMap<>();
    private final HashMap<String, Parameter<?>> parametersOut = new HashMap<>();
    private String error;

    public Procedure(Connection connection, Autoclose autoclose) {
        super(connection, autoclose);
    }

    public Procedure() {
        super();
    }

    public Procedure<T> setName(String procedureName) {
        this.procedureName = procedureName;
        return this;
    }

    private boolean errorName(String name) {
        if (error != null) return true;
        boolean error = name == null || name.trim().isEmpty();
        if (error) this.error = "Parameter name can't be null or empty!";
        return error;
    }

    private boolean errorType(String name, JDBCType type) {
        if (this.errorName(name)) return true;
        if (type != null) return false;
        error = "Parameter '" + name + "': JDBCType can't be null!";
        return true;
    }

    private boolean errorObjClass(String name, Class<?> objClass) {
        if (this.errorName(name)) return true;
        if (objClass != null) return false;
        error = "Parameter '" + name + "': objClass can't be null!";
        return true;
    }

    public Procedure<T> addParameterIn(String name, Object value, JDBCType type) {
        if (this.errorType(name, type)) return this;
        parametersIn.put(name, new Parameter<>(value, type));
        return this;
    }

    public <P> Procedure<T> addParameterIn(String name, P value, Class<P> objClass) {
        if (this.errorObjClass(name, objClass)) return this;
        parametersIn.put(name, new Parameter<>(value, objClass));
        return this;
    }

    public Procedure<T> addParameterOut(String name, JDBCType type) {
        if (this.errorType(name, type)) return this;
        parametersOut.put(name, new Parameter<>(null, type));
        return this;
    }

    public Procedure<T> addParameterOut(String name, Class<?> objClass) {
        if (this.errorObjClass(name, objClass)) return this;
        parametersOut.put(name, new Parameter<>(null, objClass));
        return this;
    }

    public String getPreparedCall() throws IOException {
        if (error != null) throw new IOException(error);
        if (procedureName == null || procedureName.trim().isEmpty())
            throw new IOException("Procedure name can't be null or empty!");
        if (procedureName.trim().split(" ").length > 1)
            throw new IOException("Procedure name can't have a blank space!");
        List<String> params = Collections.nCopies(parametersIn.size() + parametersOut.size(), "?");
        return String.format("{CALL %s(%s)}", procedureName.trim(), String.join(", ", params));
    }

    @Override
    protected SQLException buildCallableException(SQLException e) {
        String msj = String.format("%s Procedure:(%s)", e.getMessage(), String.format("{CALL %s(...)}", procedureName.trim()));
        return new SQLException(msj, e);
    }

    @Override
    protected CallableStatement executeStatement() throws IOException, SQLException {
        preparedCall = preparedCall == null ? this.getPreparedCall() : preparedCall;
        CallableStatement statement = super.getConnection().prepareCall(preparedCall);
        this.registerParameters(parametersIn, (param, name) -> param.registerIN(statement, name));
        this.registerParameters(parametersOut, (param, name) -> param.registerOUT(statement, name));
        statement.execute();
        return statement;
    }

    private void registerParameters(HashMap<String, Parameter<?>> parameters, SqlThrowingBiConsumer<Parameter<?>, String> register) throws SQLException {
        CursorIterator iterator = new CursorIterator(parameters.size());
        List<String> names = new ArrayList<>(parameters.keySet());
        try {
            for (Integer idx : iterator) {
                String name = names.get(idx);
                register.accept(parameters.get(name), name);
            }
        } catch (SQLException e) {
            String name = names.get(iterator.getPos());
            String errorMsg = "%s: Error setting '%s' parameter[type: %s] in statement! - %s";
            String type = parameters == parametersIn
                    ? (parameters.get(name).valueIsNull() ? "NULL IN" : "IN") : "OUT";
            throw new SQLException(String.format(errorMsg, procedureName, name, type, e.getMessage()), e);
        }
    }

    public T call(Supplier<T> objGenerator, ThrowingBiConsumer<T, ResultSet> mapper) throws IOException, SQLException {
        return super.execute(objGenerator, mapper);
    }

    public T call(ThrowingBiFunction<CallableStatement, ResultSet, T> executor) throws IOException, SQLException {
        CallableStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return super.run(() -> executor.apply(statement, rs));
    }

    public List<T> callForList(Supplier<T> objGenerator, ThrowingBiConsumer<T, ResultSet> mapper) throws IOException, SQLException {
        return super.executeForList(objGenerator, mapper);
    }

    public List<T> callForList(ThrowingBiFunction<CallableStatement, ResultSet, List<T>> executor) throws IOException, SQLException {
        CallableStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return super.runForList(() -> executor.apply(statement, rs));
    }

    public void call() throws IOException, SQLException {
        try {
            this.executeStatement();
        } finally {
            super.close();
        }
    }
}
