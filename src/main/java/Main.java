import example.TaskExecutor;
import javafx.util.Pair;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.concurrent.Callable;

public class Main {

    public static void main(String... args) throws Exception {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.start();
        taskExecutor.accept(new Pair<>(new DateTime(), Main::computePi));
    }

    public static double computePi() {
        double pi = 1;
        for (long i = 3; i < 5_000_000_000L; i += 4) {
            pi = pi - (1 / (double) i) + (1 / (double) (i + 2));
        }
        return pi * 4;
    }

}
