package pe.mrodas.jdbc.helper;

import java.sql.Statement;

public enum GeneratedKeys {
    RETURN(Statement.RETURN_GENERATED_KEYS), NO_RETURN(Statement.NO_GENERATED_KEYS);
    private final int value;

    GeneratedKeys(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
