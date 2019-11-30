package pe.mrodas.jdbc.helper;

@FunctionalInterface
public interface ThrowingBiFunction<S, T, R> {
    R apply(S arg1, T arg2) throws Exception;
}
