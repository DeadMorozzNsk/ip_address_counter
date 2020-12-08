import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class EfficiencyTest {

    @Test
    public void timeTest() {
        int[] partOne = new int[256];
        int[] partTwo = new int[256];
        int[] partThree = new int[256];
        int[] partFour = new int[256];

        int[][] stats = new int[4][256];
        stats[0] = partOne;
        stats[1] = partTwo;
        stats[2] = partThree;
        stats[3] = partFour;

        String path = "src\\main\\java\\org\\ecwid\\ipcounter\\files\\testSet.txt";
        long t;
        AtomicInteger validStringsCount = new AtomicInteger();
        t = System.currentTimeMillis();
        //AtomicLong millis = new AtomicLong();
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            lines.parallel()
                    .filter(this::addressIsValid)
                    .forEach(//);
            /*lines.forEach(*/(ipAddress) -> {
                //long k= System.currentTimeMillis();
                //if (addressIsValid(ipAddress)) {
                //    millis.addAndGet(System.currentTimeMillis() - k);
                    String[] parts = ipAddress.split("\\.");
                    for (int j = 0; j < 4; j++) {
                        stats[j][Integer.parseInt(parts[j])] += 1;
                    }
                    validStringsCount.getAndIncrement();
                //}
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        //System.out.println("regex matching = " + millis + "ms");

        System.out.println("file reading ended " + (System.currentTimeMillis() - t) + "ms");
    }

    private boolean addressIsValid(String address) {
        return address.matches("\\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.|$)){4}\\b");
    }
}
