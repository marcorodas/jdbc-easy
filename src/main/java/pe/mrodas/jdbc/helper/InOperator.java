package pe.mrodas.jdbc.helper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class InOperator<T> {
    private String fields;
    private final Map<String, T> parameters;

    public InOperator(String name, List<T> list) {
        this(name, list == null ? null : list.stream());
    }

    public InOperator(String name, Stream<T> ids) {
        if (ids == null) parameters = null;
        else if (name == null) parameters = null;
        else {
            List<T> list = ids.filter(Objects::nonNull).collect(Collectors.toList());
            if (list.isEmpty()) parameters = null;
            else {
                fields = IntStream.range(0, list.size())
                        .mapToObj(i -> ":" + name + i)
                        .collect(Collectors.joining(","));
                parameters = new HashMap<>();
                for (int i = 0; i < list.size(); i++)
                    parameters.put(name + i, list.get(i));
            }
        }
    }

    public String getFields() {
        return fields;
    }

    public Map<String, T> getParameters() {
        return isInvalid() ? Collections.emptyMap() : parameters;
    }

    public boolean isInvalid() {
        return parameters == null;
    }
}
