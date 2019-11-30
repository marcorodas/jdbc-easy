package pe.mrodas.jdbc.helper;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T arg) throws Exception;
}