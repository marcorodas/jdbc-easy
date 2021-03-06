package pe.mrodas.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import pe.mrodas.jdbc.helper.ThrowingConsumer;
import pe.mrodas.jdbc.helper.ThrowingFunction;

public class Connector {

    private static Config initConfig;
    private static Connector connector;
    private final Properties dbInfo;
    private final String url;

    public Connector(Config config) throws IOException {
        this.dbInfo = (config == null ? new Config("db.properties") : config).getProperties();
        this.url = dbInfo.getProperty("url");
        if (this.url == null) throw new IOException("Missing url property!");
        String driver = dbInfo.getProperty("driver");
        if (driver == null) throw new IOException("Missing driver property!");
        try {
            if (driver.contains("mysql")) try {
                Class.forName(driver.replace("mysql.", "mysql.cj."));
            } catch (ClassNotFoundException e) {
                Class.forName(driver);
            }
            else Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new IOException(String.format("Class '%s' not found!", driver));
        } catch (Exception e) {
            throw new IOException(String.format("Class '%s' initialization fail!", driver));
        }
    }

    public Connection getConn() throws SQLException {
        return DriverManager.getConnection(url, dbInfo);
    }

    public static Connection getConnection() throws SQLException, IOException {
        if (connector == null) connector = new Connector(initConfig);
        return connector.getConn();
    }

    public static void batch(ThrowingConsumer<Connection> consumer) throws Exception {
        try (Connection connection = Connector.getConnection()) {
            connection.setAutoCommit(false);
            try {
                consumer.accept(connection);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    public static <T> T batch(ThrowingFunction<Connection, T> function) throws Exception {
        try (Connection connection = Connector.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = function.apply(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    /**
     * Sample Use:
     * <pre>
     * {@code
     * Connector.configure(new String[]{
     *      "url = jdbc:mysql://127.0.0.1:3306/dbName",
     *      "driver = com.mysql.jdbc.Driver",
     *      "user = userName",
     *      "password = pass"
     * });
     * }</pre>
     *
     * @param connectionString Propiedad: "key = value"
     */
    public static void configure(String[] connectionString) {
        initConfig = new Config(connectionString);
    }

    /**
     * connectionString:
     * <pre>
     * {@code
     * "url = jdbc:mysql://127.0.0.1:3306/dbName; driver = com.mysql.jdbc.Driver; user = userName; password = pass"
     * }</pre>
     *
     * @param connectionString Propiedad: "key = value"
     */
    public static void configure(String connectionString) {
        initConfig = new Config(connectionString == null ? null : connectionString.split(";"));
    }

    /**
     * Properties:
     * <pre>
     * {@code
     *  url = jdbc\:mysql\://127.0.0.1:3306/sgsi
     *  driver = com.mysql.jdbc.Driver
     *  user = userName
     *  password = pass
     * });
     * }</pre>
     *
     * @param properties input properties
     */
    public static void configure(Properties properties) {
        initConfig = new Config(properties);
    }

    /**
     * Archivo ubicado generalmente en src\main\resources
     * <p>
     * Debe contener las siguientes propiedades para la conexión:</p>
     * <pre>
     * {@code
     * url=jdbc:mysql://127.0.0.1:3306/dbName
     * driver=com.mysql.jdbc.Driver
     * user=userName
     * password=pass
     * }</pre>
     *
     * @param propertyFile Nombre de archivo de propiedades ej:
     *                     "db.properties"
     */
    public static void configure(File propertyFile) {
        initConfig = new Config(propertyFile == null ? null : propertyFile.getName());
    }
}