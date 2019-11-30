package pe.mrodas.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private Properties properties;
    private String propertiesFile, error;

    public Config(String[] connectionString) {
        if (connectionString == null) this.error = "new Config(): ConnectionString can't be null!";
        else {
            properties = new Properties();
            for (String part : connectionString)
                if (part.contains("=")) {
                    String[] property = part.split("=");
                    properties.setProperty(property[0].trim(), property[1].trim());
                }
        }
    }

    public Config(Properties properties) {
        if (properties == null) this.error = "new Config(): Properties can't be null!";
        else this.properties = properties;
    }

    public Config(String propertiesFile) {
        if (propertiesFile == null) this.error = "new Config(): PropertiesFile can't be null!";
        else this.propertiesFile = propertiesFile;
    }

    public Properties getProperties() throws IOException {
        if (error != null) throw new IOException(error);
        if (properties == null) {
            properties = new Properties();
            ClassLoader cl = getClass().getClassLoader();
            try (InputStream stream = cl.getResourceAsStream(propertiesFile)) {
                if (stream == null)
                    throw new IOException(String.format("File '%s' doesn't exists!", propertiesFile));
                properties.load(stream);
            } catch (IllegalArgumentException e) {
                throw new IOException(String.format("The input stream of '%s' contains a malformed Unicode escape sequence! - ", propertiesFile) + e.getMessage());
            }
        }
        return properties;
    }
}
