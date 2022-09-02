package be.twofold.stackoverflow.model;

import java.util.Locale;
import java.util.regex.Pattern;

public interface Named {

    Pattern CAMEL_CASE = Pattern.compile("([a-z])([A-Z])");

    String getName();

    default String getPgName() {
        return CAMEL_CASE.matcher(getName())
            .replaceAll("$1_$2")
            .toLowerCase(Locale.ROOT);
    }

}
