package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class RunnableExecutorTest {

    private final static Logger logger = Logger.getLogger(RunnableExecutorTest.class);
    private final static long INTERVAL = 2_000;
    private final static int CNT = 100;

    @Test
    public void testWakeUp() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        Thread.sleep(1_000);
        long now = System.currentTimeMillis();
        DateTime time1 = new DateTime(now + 10_000);
        DateTime time2 = new DateTime(now + 15_000);
        Future<Double> res2 = taskExecutor.apply(time2, new TestTask(2)::slow);
        Thread.sleep(5_000);
        Future<Double> res1 = taskExecutor.apply(time1, new TestTask(1)::slow);
        logger.info(res1.get());
        logger.info(res2.get());
        taskExecutor.safeStop();
    }

    @Test
    public void testOrder() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        long now = System.currentTimeMillis();
        Putter putter1 = new Putter(taskExecutor, new AtomicLong(0), now);
        Putter putter2 = new Putter(taskExecutor, new AtomicLong(100), now);
        new Thread(putter1::put).start();
        new Thread(putter2::put).start();
        Future<Double> res1 = taskExecutor.apply(new DateTime(now + 12_000), new TestTask(0)::slow);
        res1.get();
        taskExecutor.safeStop();
    }

    @Test
    public void testHighLoad() throws Exception {
        TaskExecutor<Double> taskExecutor = new TaskExecutor<>();
        taskExecutor.start();
        Future<Double> res1 = taskExecutor.apply(new DateTime(System.currentTimeMillis() + INTERVAL * 2 * CNT + 1_000), new TestTask(0)::slow);
        Putter putter = new Putter(taskExecutor, new AtomicLong(0), System.currentTimeMillis());
        new Thread(putter::putRandom).start();
        new Thread(putter::putRandom).start();
        new Thread(putter::putRandom).start();
        new Thread(putter::putRandom).start();
        res1.get();
        taskExecutor.safeStop();
    }

    private class TestTask {

        private final long num;

        private TestTask(long num) {
            this.num = num;
        }

        public Double slow() throws Exception {
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



    private class Putter {

        final TaskExecutor<Double> taskExecutor;
        final AtomicLong counter;
        final long now;

        public Putter(TaskExecutor<Double> taskExecutor, AtomicLong counter, long now) {
            this.taskExecutor = taskExecutor;
            this.counter = counter;
            this.now = now;
        }

        private void put(long offset) {
            taskExecutor.apply(new DateTime(now + offset), new TestTask(counter.incrementAndGet())::slow);
        }

        private void put() {
            for (int i = 0; i < 10; i++)
                put(5_000);
            for (int i = 0; i < 10; i++)
                put(8_000);
        }

        private void putRandom() {
            long range = INTERVAL * 2 * CNT;
            long offset = 0;
            for (int i = 0; i < CNT; i++)
                try {
                    long tmp = (long) (INTERVAL * 2 * Math.random());
                    Thread.sleep(tmp);
                    range -= tmp;
                    offset += tmp;
                    put(offset + (long) (range * Math.random()));
                } catch (InterruptedException ignored) {
                }
        }

    }

}