package be.twofold.stackoverflow;

import javax.xml.stream.*;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import java.util.zip.*;

public final class SoImport {

    // region Tables

    private static final Table Badges = new Table("badges", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("UserId", Types.INTEGER),
        Column.variable("Name", Types.VARCHAR, 50),
        Column.fixed("Date", Types.TIMESTAMP),
        Column.fixed("Class", Types.SMALLINT),
        Column.fixed("TagBased", Types.BOOLEAN)
    ));

    private static final Table Comments = new Table("comments", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("PostId", Types.INTEGER),
        Column.fixed("Score", Types.INTEGER),
        Column.variable("Text", Types.VARCHAR, 600),
        Column.fixed("CreationDate", Types.TIMESTAMP),
        Column.variable("UserDisplayName", Types.VARCHAR, 40).withNulls(),
        Column.fixed("UserId", Types.INTEGER),
        Column.variable("ContentLicense", Types.VARCHAR, 12)
    ));

    private static final Table PostLinks = new Table("postLinks", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("CreationDate", Types.TIMESTAMP),
        Column.fixed("PostId", Types.INTEGER),
        Column.fixed("RelatedPostId", Types.INTEGER),
        Column.fixed("LinkTypeId", Types.SMALLINT)
    ));

    private static final Table Posts = new Table("posts", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("PostTypeId", Types.SMALLINT),
        Column.fixed("AcceptedAnswerId", Types.INTEGER),
        Column.fixed("ParentId", Types.INTEGER).withNulls(),
        Column.fixed("CreationDate", Types.TIMESTAMP),
        Column.fixed("DeletionDate", Types.TIMESTAMP).withNulls(),
        Column.fixed("Score", Types.INTEGER),
        Column.fixed("ViewCount", Types.INTEGER),
        Column.variable("Body", Types.VARCHAR, Integer.MAX_VALUE),
        Column.fixed("OwnerUserId", Types.INTEGER),
        Column.variable("OwnerDisplayName", Types.VARCHAR, 40).withNulls(),
        Column.fixed("LastEditorUserId", Types.INTEGER),
        Column.variable("LastEditorDisplayName", Types.VARCHAR, 40),
        Column.fixed("LastEditDate", Types.TIMESTAMP),
        Column.fixed("LastActivityDate", Types.TIMESTAMP),
        Column.variable("Title", Types.VARCHAR, 250),
        Column.variable("Tags", Types.VARCHAR, 250),
        Column.fixed("AnswerCount", Types.INTEGER),
        Column.fixed("CommentCount", Types.INTEGER),
        Column.fixed("FavoriteCount", Types.INTEGER),
        Column.fixed("ClosedDate", Types.TIMESTAMP).withNulls(),
        Column.fixed("CommunityOwnedDate", Types.TIMESTAMP).withNulls(),
        Column.variable("ContentLicense", Types.VARCHAR, 12)
    ));

    private static final Table Tags = new Table("tags", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.variable("TagName", Types.VARCHAR, 35),
        Column.fixed("Count", Types.INTEGER),
        Column.fixed("ExcerptPostId", Types.INTEGER),
        Column.fixed("WikiPostId", Types.INTEGER),
        Column.fixed("IsModeratorOnly", Types.BOOLEAN).withNulls(),
        Column.fixed("IsRequired", Types.BOOLEAN).withNulls()
    ));

    private static final Table Users = new Table("users", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("Reputation", Types.INTEGER),
        Column.fixed("CreationDate", Types.TIMESTAMP),
        Column.variable("DisplayName", Types.VARCHAR, 40),
        Column.fixed("LastAccessDate", Types.TIMESTAMP),
        Column.variable("WebsiteUrl", Types.VARCHAR, 200).withNulls(),
        Column.variable("Location", Types.VARCHAR, 100).withNulls(),
        Column.variable("AboutMe", Types.VARCHAR, Integer.MAX_VALUE),
        Column.fixed("Views", Types.INTEGER),
        Column.fixed("UpVotes", Types.INTEGER),
        Column.fixed("DownVotes", Types.INTEGER),
        Column.variable("ProfileImageUrl", Types.VARCHAR, 200).withNulls(),
        Column.variable("EmailHash", Types.VARCHAR, 32).withNulls(),
        Column.fixed("AccountId", Types.INTEGER).withNulls()
    ));

    private static final Table Votes = new Table("votes", List.of(
        Column.fixed("Id", Types.INTEGER),
        Column.fixed("PostId", Types.INTEGER),
        Column.fixed("VoteTypeId", Types.SMALLINT),
        Column.fixed("UserId", Types.INTEGER).withNulls(),
        Column.fixed("CreationDate", Types.TIMESTAMP),
        Column.fixed("BountyAmount", Types.INTEGER).withNulls()
    ));

    private static final List<Table> Tables = List.of(
        Badges,
        Comments,
        PostLinks,
        Posts,
        Tags,
        Users,
        Votes
    );

    // endregion

    // region SQL

    private static String createSql(Table table) {
        String columns = table.columns().stream()
            .map(column -> pgName(column.name()) + " " + getType(column) + (column.nullable() ? "" : " not null"))
            .collect(Collectors.joining(", "));

        return "create table if not exists " + pgName(table.name()) + " (" + columns + ");";
    }

    private static String insertSql(Table table) {
        String params = table.columns().stream()
            .map(column -> pgName(column.name()))
            .collect(Collectors.joining(", "));

        String values = IntStream.range(0, table.columns().size())
            .mapToObj(__ -> "?")
            .collect(Collectors.joining(", "));

        return "insert into " + pgName(table.name()) + " (" + params + ") values (" + values + ");";
    }

    private static String getType(Column column) {
        return switch (column.type()) {
            case Types.BOOLEAN -> "boolean";
            case Types.INTEGER -> "int";
            case Types.SMALLINT -> "smallint";
            case Types.TIMESTAMP -> "timestamp";
            case Types.VARCHAR -> column.length() != Integer.MAX_VALUE ? "varchar(" + column.length() + ")" : "text";
            default -> throw new IllegalStateException("Unexpected value: " + column.type());
        };
    }

    // endregion

    // region Mapping

    @FunctionalInterface
    public interface PreparedStatementMapper {
        void set(PreparedStatement stmt, String value) throws SQLException;
    }

    private static PreparedStatementMapper mapper(int type, int index) {
        return switch (type) {
            case Types.BOOLEAN -> (stmt, value) -> stmt.setBoolean(index, parseBoolean(value));
            case Types.INTEGER -> (stmt, value) -> stmt.setInt(index, Integer.parseInt(value));
            case Types.SMALLINT -> (stmt, value) -> stmt.setShort(index, Short.parseShort(value));
            case Types.TIMESTAMP -> (stmt, value) -> stmt.setTimestamp(index, parseTimestamp(value));
            case Types.VARCHAR -> (stmt, value) -> stmt.setString(index, value);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }

    private static boolean parseBoolean(String value) {
        return switch (value) {
            case "True" -> true;
            case "False" -> false;
            default -> throw new IllegalStateException("Unexpected value: " + value);
        };
    }

    private static Timestamp parseTimestamp(String value) {
        return Timestamp.valueOf(LocalDateTime.parse(value));
    }

    // endregion

    public static void main(String[] args) {
        Path root = Paths.get("C:\\Temp\\math.stackexchange.com");
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/so?reWriteBatchedInserts=true", "so", "so")) {
            conn.setAutoCommit(false);

            for (Table table : Tables) {
                create(conn, table);
                insert(conn, table, root.resolve(table.name() + ".xml.gz"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SQLException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void create(Connection conn, Table table) throws SQLException {
        System.out.println("Creating table for '" + table.name() + "'");

        try (Statement stmt = conn.createStatement()) {
            String sql = createSql(table);
            stmt.executeUpdate(sql);
        }
    }

    private static void insert(Connection conn, Table table, Path path) throws IOException, SQLException, XMLStreamException {
        System.out.println("Inserting table for '" + table.name() + "'");

        try (InputStream input = new GZIPInputStream(Files.newInputStream(path));
             PreparedStatement stmt = conn.prepareStatement(insertSql(table))
        ) {
            Map<Integer, Integer> nullables = createNullables(table);
            Map<String, PreparedStatementMapper> mappers = createMappers(table);

            ProgressMonitor monitor = new ProgressMonitor();
            XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
            while (reader.hasNext()) {
                reader.next();
                if (reader.isStartElement() && "row".equals(reader.getName().getLocalPart())) {
                    for (Map.Entry<Integer, Integer> entry : nullables.entrySet()) {
                        stmt.setNull(entry.getKey(), entry.getValue());
                    }
                    for (int i = 0, len = reader.getAttributeCount(); i < len; i++) {
                        mappers
                            .get(reader.getAttributeName(i).getLocalPart())
                            .set(stmt, reader.getAttributeValue(i));
                    }
                    stmt.addBatch();
                    int count = monitor.incrementCount();
                    if ((count % 1024) == 0) {
                        commit(conn, stmt);
                    }
                }
            }
            monitor.print();
            commit(conn, stmt);
        }
    }

    private static void commit(Connection conn, Statement stmt) throws SQLException {
        stmt.executeBatch();
        conn.commit();
    }

    private static Map<Integer, Integer> createNullables(Table table) {
        Map<Integer, Integer> nullables = new HashMap<>();
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            if (column.nullable()) {
                nullables.put(i + 1, column.type());
            }
        }
        return Map.copyOf(nullables);
    }

    private static Map<String, PreparedStatementMapper> createMappers(Table table) {
        Map<String, PreparedStatementMapper> mappers = new HashMap<>();
        for (int i = 0; i < table.columns().size(); i++) {
            Column column = table.columns().get(i);
            mappers.put(column.name(), mapper(column.type(), i + 1));
        }
        return Map.copyOf(mappers);
    }

    // region Model

    private static final Pattern CAMEL_CASE = Pattern.compile("([a-z])([A-Z])");

    private static String pgName(String name) {
        return CAMEL_CASE.matcher(name).replaceAll("$1_$2").toLowerCase(Locale.ROOT);
    }

    public record Table(String name, List<Column> columns) {
        public Table(String name, List<Column> columns) {
            this.name = Objects.requireNonNull(name);
            this.columns = List.copyOf(columns);
        }
    }

    public record Column(String name, int type, Integer length, boolean nullable) {
        public Column {
            Objects.requireNonNull(name);
        }

        public static Column fixed(String name, int type) {
            return new Column(name, type, 0, false);
        }

        public static Column variable(String name, int type, int length) {
            return new Column(name, type, length, false);
        }

        public Column withNulls() {
            return new Column(name, type, length, true);
        }
    }

    // endregion

}
