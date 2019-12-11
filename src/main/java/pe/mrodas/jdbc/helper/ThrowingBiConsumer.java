package pe.mrodas.jdbc.helper;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {
    void accept(T t, U u) throws Exception;
}
