import demo.TaskExecutor;
import org.joda.time.DateTime;

import java.util.concurrent.*;

public class Main {

    public static void main(String... args) throws Exception {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.start();
        taskExecutor.accept(new DateTime(), Main::computePi);

        ScheduledExecutorService d;
        ScheduledThreadPoolExecutor s;
        s.schedule();
        PriorityBlockingQueue x = new PriorityBlockingQueue<>();
        ExecutorService z = Executors.newFixedThreadPool(10);
//        z = new ThreadPoolExecutor(10, 10,
//                0L, TimeUnit.MILLISECONDS,
//                new LinkedBlockingQueue<>());
        z.submit();
    }

    public static double computePi() {
        double pi = 1;
        for (long i = 3; i < 5_000_000_000L; i += 4) {
            pi = pi - (1 / (double) i) + (1 / (double) (i + 2));
        }
        return pi * 4;
    }

}
