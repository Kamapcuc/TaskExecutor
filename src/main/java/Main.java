import example.TaskExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Main {

    public static void main(String... args) throws Exception {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.start();
        taskExecutor.accept(new DateTime(), Main::computePi);

        ScheduledExecutorService d;
        ScheduledThreadPoolExecutor s;
        PriorityBlockingQueue x = new PriorityBlockingQueue<>();
    }

    public static double computePi() {
        double pi = 1;
        for (long i = 3; i < 5_000_000_000L; i += 4) {
            pi = pi - (1 / (double) i) + (1 / (double) (i + 2));
        }
        return pi * 4;
    }

}
