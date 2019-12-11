package pe.mrodas.jdbc.helper;

@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
