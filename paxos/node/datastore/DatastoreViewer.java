import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatastoreViewer {
    static String FILE_NUMBER = "1";

    public static void main(String[] args) {
        String fileNumber = FILE_NUMBER;
        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
            fileNumber = args[0];
        }

        Path dir = Paths.get(".").toAbsolutePath().normalize();

        try {
            List<Path> files = Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".mapdb"))
                    .sorted()
                    .collect(Collectors.toList());
            System.out.println("MapDB files in " + dir + ":");
            for (Path p : files) {
                System.out.println(" - " + p.getFileName());
            }
        } catch (IOException e) {
            System.out.println("Failed to list files: " + e.getMessage());
        }

        int n;
        try {
            n = Integer.parseInt(fileNumber);
        } catch (NumberFormatException e) {
            System.out.println("Invalid FILE_NUMBER: " + fileNumber);
            return;
        }

        String fname = String.format("node-%d-db.mapdb", n);
        Path f = dir.resolve(fname);
        if (!Files.exists(f)) {
            System.out.println("DB file not found: " + f.toAbsolutePath());
            return;
        }

        DB db = DBMaker
                .fileDB(f.toFile())
                .readOnly()
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .make();
        try {
            HTreeMap<Integer, Long> accounts = db
                    .hashMap("accounts", Serializer.INTEGER, Serializer.LONG)
                    .open();

            List<Map.Entry<Integer, Long>> entries = new ArrayList<>(accounts.entrySet());
            entries.sort(Comparator.comparingInt(Map.Entry::getKey));
            System.out.println("-- accounts --");
            for (Map.Entry<Integer, Long> e : entries) {
                System.out.println(e.getKey() + "," + e.getValue());
            }
        } finally {
            db.close();
        }
    }
}
