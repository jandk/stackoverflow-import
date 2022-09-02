package be.twofold.stackoverflow.model;

import java.util.List;
import java.util.Objects;

public final class Table implements Named {
    private final String name;
    private final List<Column> columns;

    public Table(String name, List<Column> columns) {
        this.name = Objects.requireNonNull(name);
        this.columns = List.copyOf(columns);
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Table)) return false;

        Table other = (Table) obj;
        return name.equals(other.name)
            && columns.equals(other.columns);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + name.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Table(" +
            "name=" + name + ", " +
            "columns=" + columns +
            ")";
    }
}
