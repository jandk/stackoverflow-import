package be.twofold.stackoverflow;

import be.twofold.stackoverflow.model.Column;
import be.twofold.stackoverflow.model.Table;
import be.twofold.stackoverflow.util.ProgressMonitor;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

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
        String columns = table.getColumns().stream()
            .map(column -> column.getPgName() + " " + getType(column) + (column.isNullable() ? "" : " not null"))
            .collect(Collectors.joining(", "));

        return "create table if not exists " + table.getPgName() + " (" + columns + ");";
    }

    private static String insertSql(Table table) {
        String params = table.getColumns().stream()
            .map(Column::getPgName)
            .collect(Collectors.joining(", "));

        String values = IntStream.range(0, table.getColumns().size())
            .mapToObj(__ -> "?")
            .collect(Collectors.joining(", "));

        return "insert into " + table.getPgName() + " (" + params + ") values (" + values + ");";
    }

    private static String getType(Column column) {
        switch (column.getSqlType()) {
            case Types.BOOLEAN:
                return "boolean";
            case Types.INTEGER:
                return "int";
            case Types.SMALLINT:
                return "smallint";
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.VARCHAR:
                return column.getLength() != Integer.MAX_VALUE
                    ? "varchar(" + column.getLength() + ")"
                    : "text";
            default:
                throw new IllegalStateException("Unexpected value: " + column.getSqlType());
        }
    }

    // endregion

    // region Mapping

    @FunctionalInterface
    public interface PreparedStatementMapper {
        void set(PreparedStatement stmt, String value) throws SQLException;
    }

    private static PreparedStatementMapper mapper(int type, int index) {
        switch (type) {
            case Types.BOOLEAN:
                return (stmt, value) -> stmt.setBoolean(index, parseBoolean(value));
            case Types.INTEGER:
                return (stmt, value) -> stmt.setInt(index, Integer.parseInt(value));
            case Types.SMALLINT:
                return (stmt, value) -> stmt.setShort(index, Short.parseShort(value));
            case Types.TIMESTAMP:
                return (stmt, value) -> stmt.setTimestamp(index, parseTimestamp(value));
            case Types.VARCHAR:
                return (stmt, value) -> stmt.setString(index, value);
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    private static boolean parseBoolean(String value) {
        switch (value) {
            case "True":
                return true;
            case "False":
                return false;
            default:
                throw new IllegalStateException("Unexpected value: " + value);
        }
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
                insert(conn, table, root.resolve(table.getName() + ".xml.gz"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SQLException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void create(Connection conn, Table table) throws SQLException {
        System.out.println("Creating table for '" + table.getName() + "'");

        try (Statement stmt = conn.createStatement()) {
            String sql = createSql(table);
            stmt.executeUpdate(sql);
        }
    }

    private static void insert(Connection conn, Table table, Path path) throws IOException, SQLException, XMLStreamException {
        System.out.println("Inserting table for '" + table.getName() + "'");

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
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column column = table.getColumns().get(i);
            if (column.isNullable()) {
                nullables.put(i + 1, column.getSqlType());
            }
        }
        return Map.copyOf(nullables);
    }

    private static Map<String, PreparedStatementMapper> createMappers(Table table) {
        Map<String, PreparedStatementMapper> mappers = new HashMap<>();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column column = table.getColumns().get(i);
            mappers.put(column.getName(), mapper(column.getSqlType(), i + 1));
        }
        return Map.copyOf(mappers);
    }

}
