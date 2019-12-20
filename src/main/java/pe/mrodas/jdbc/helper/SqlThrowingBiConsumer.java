package pe.mrodas.jdbc.helper;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlThrowingBiConsumer<T, U> {
    void accept(T t, U u) throws SQLException;
}
