package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RunnableExecutorTest {

    private final static Logger logger = Logger.getLogger(RunnableExecutorTest.class);

    @Test
    public void testWakeUp() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        Thread.sleep(1_000);
        long now = System.currentTimeMillis();
        DateTime time1 = new DateTime(now + 15_000);
        DateTime time2 = new DateTime(now + 10_000);
        Future<Double> res1 = taskExecutor.apply(time1, new SlowCallable(1));
        Thread.sleep(5_000);
        Future<Double> res2 = taskExecutor.apply(time2, new SlowCallable(2));
        logger.info(res2.get());
        logger.info(res1.get());
        taskExecutor.safeStop();
    }

    @Test
    public void testOrder() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        counter.incrementAndGet();
    }

    @Test
    public void testHighLoad() throws Exception {
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
            logger.info(String.format("%s started task №%d execution", Thread.currentThread().getName(), num));
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