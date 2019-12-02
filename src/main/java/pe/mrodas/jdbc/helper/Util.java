package pe.mrodas.jdbc.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class Util {

    public static List<Integer> parseIdList(String idList) {
        return Util.parseIdList(idList, null);
    }

    public static List<Integer> parseIdList(String idList, String separator) {
        if (idList == null || idList.isEmpty()) return new ArrayList<>();
        return Arrays.stream(idList.split(separator == null || separator.isEmpty() ? "," : separator))
                .filter(id -> id.trim().matches("\\d+"))
                .map(id -> Integer.parseInt(id.trim()))
                .collect(Collectors.toList());
    }

}
