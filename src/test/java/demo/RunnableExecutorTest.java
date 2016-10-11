package demo;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RunnableExecutorTest {

    @Test
    public void testNotify() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        DateTime future = new DateTime(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10));
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        Future<Double> tt = taskExecutor.apply(future, new SlowCallable(1));
        System.out.println(tt.get());
        taskExecutor.safeStop();
    }

    @Test
    public void testWakeUp() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        DateTime future1 = new DateTime(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(60));
        Future<Double> res1 = taskExecutor.apply(future1, new SlowCallable(1));
        DateTime future2 = new DateTime(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10));
        Future<Double> res2 = taskExecutor.apply(future2, new SlowCallable(2));
        System.out.println(res2.get());
        System.out.println(res1.get());
        taskExecutor.safeStop();
    }

    @Test
    public void testOrder() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        counter.incrementAndGet();
    }


    private class SlowCallable implements Callable<Double> {

        private final long num;

        private SlowCallable(long num) {
            this.num = num;
        }

        @Override
        public Double call() throws Exception {
            System.out.println(String.format("Starting task №%d", num));
            long accuracy = 2_000_000_000L + (long) Math.floor(3_000_000_000L * Math.random());
            return computePi(accuracy);
        }

        public double computePi(long accuracy) {
            double pi = 1;
            for (long i = 3; i < accuracy; i += 4) {
                pi = pi - (1 / (double) i) + (1 / (double) (i + 2));
            }
            return pi * 4;
        }

    }



}