import example.TaskExecutor;

import java.util.Date;
import java.util.concurrent.Callable;

public class Main {

    public static void main(String... args) throws Exception {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.start();
        Callable<Double> x = Main::computePi;
        Date z = null;
        System.out.println(x.call());
    }

    public static double computePi() {
        double pi = 1;
        for (long i = 3; i < 5_000_000_000L; i += 4) {
            pi = pi - (1 / (double) i) + (1 / (double) (i + 2));
        }
        return pi * 4;
    }

}
