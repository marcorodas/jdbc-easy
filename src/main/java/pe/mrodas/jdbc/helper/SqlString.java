package pe.mrodas.jdbc.helper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convenience class to handle 2 situations:
 * 1. Converts .sql to String[] for SqlQuery<...>.setSql(new String[]{...})
 * 2. Converts String[] to .sql
 *
 * @author Marco Rodas
 */
public class SqlString {

    private final List<String> lines;

    public SqlString(String file) throws InvalidPathException, IOException {
        this.lines = Files.readAllLines(Paths.get(file), Charset.defaultCharset());
    }

    public SqlString(String... lines) {
        this.lines = Arrays.asList(lines);
    }

    public String toStrArray() {
        return lines.stream().map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(",\n"));
    }

    public String toSql() {
        List<String> sqlLines = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            int endIndex = i == lines.size() - 1 ? 1 : 2;
            sqlLines.add(line.substring(1, line.length() - endIndex));
        }
        return String.join("\n", sqlLines);
    }

    public static String arrayToSql(String... strings) {
        return String.join("\n", strings);
    }

    public static String strToSqlArray(String sql) {
        String arr = Stream.of(sql.split("\n")).map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(",\n"));
        return String.format("new String[]{\n%s\n}", arr);
    }
}
