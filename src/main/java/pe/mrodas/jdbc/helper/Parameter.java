package pe.mrodas.jdbc.helper;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

public class Parameter<P> {

    private final P value;
    private Class<P> pClass;
    private JDBCType type;

    public Parameter(P value, JDBCType type) {
        this.value = value;
        this.type = type;
    }

    public Parameter(P value, Class<P> objClass) {
        this.value = value;
        this.pClass = objClass;
    }

    public Parameter(P value) {
        this.value = value;
    }

    public boolean valueIsNull() {
        return value == null;
    }

    private JDBCType getJDBCType(Class<?> objClass) throws SQLException {
        if (objClass == null) return null;
        if (objClass.isArray()) {
            Class<?> componentType = objClass.getComponentType();
            if (componentType != null && componentType.isPrimitive())
                if (byte.class.isAssignableFrom(componentType))
                    return JDBCType.BLOB;
        }
        if (objClass == Integer.class) return JDBCType.INTEGER;
        if (objClass == String.class) return JDBCType.VARCHAR;
        if (objClass == Boolean.class) return JDBCType.BOOLEAN;
        if (objClass == Double.class) return JDBCType.DOUBLE;
        if (objClass == Float.class) return JDBCType.FLOAT;
        if (objClass == InputStream.class) return JDBCType.BLOB;
        if (objClass == Date.class) return JDBCType.TIMESTAMP;
        if (objClass == Time.class) return JDBCType.TIME;
        if (objClass == Timestamp.class) return JDBCType.TIMESTAMP;
        if (objClass == LocalDate.class) return JDBCType.DATE;
        if (objClass == LocalTime.class) return JDBCType.TIME;
        if (objClass == LocalDateTime.class) return JDBCType.TIMESTAMP;
        throw new SQLException("Unable to find JDBCType for '" + objClass.getName() + "' class");
    }

    public Integer getSqlType() throws SQLException {
        JDBCType jdbcType = type == null ? this.getJDBCType(pClass) : type;
        if (jdbcType == null)
            throw new SQLException("JDBCType or Class must be defined as NOT NULL in constructor ParamValue!");
        return jdbcType.getVendorTypeNumber();
    }

    public void registerIN(CallableStatement statement, String name) throws SQLException {
        if (value == null) statement.setNull(name, this.getSqlType());
        else {
            Class<?> objClass = pClass == null ? value.getClass() : pClass;
            if (objClass.isArray()) {
                Class<?> componentType = objClass.getComponentType();
                if (componentType != null && componentType.isPrimitive())
                    if (byte.class.isAssignableFrom(componentType))
                        statement.setBytes(name, (byte[]) value);
            } else if (objClass == Integer.class) statement.setInt(name, (Integer) value);
            else if (objClass == String.class) statement.setString(name, (String) value);
            else if (objClass == Boolean.class) statement.setBoolean(name, (Boolean) value);
            else if (objClass == Double.class) statement.setDouble(name, (Double) value);
            else if (objClass == Float.class) statement.setFloat(name, (Float) value);
            else if (value instanceof InputStream) statement.setBlob(name, (InputStream) value);
            else if (objClass == Date.class) {
                long time = ((Date) value).getTime();
                statement.setTimestamp(name, new Timestamp(time));
            } else if (objClass == Time.class) statement.setTime(name, (Time) value);
            else if (objClass == Timestamp.class) statement.setTimestamp(name, (Timestamp) value);
            else if (objClass == LocalDate.class)
                statement.setDate(name, java.sql.Date.valueOf((LocalDate) value));
            else if (objClass == LocalTime.class) statement.setTime(name, Time.valueOf((LocalTime) value));
            else if (objClass == LocalDateTime.class)
                statement.setTimestamp(name, Timestamp.valueOf((LocalDateTime) value));
        }
    }

    public void registerIN(PreparedStatement statement, int index) throws SQLException {
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

    public void registerOUT(CallableStatement statement, String name) throws SQLException {
        JDBCType jdbcType = type == null ? this.getJDBCType(pClass) : type;
        if (jdbcType != null) statement.registerOutParameter(name, jdbcType.getVendorTypeNumber());
        else throw new SQLException("JDBCType or Class must be defined as NOT NULL in constructor ParamValue!");
    }

}
