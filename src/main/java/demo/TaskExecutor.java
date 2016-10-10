package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class TaskExecutor extends Thread implements BiFunction<DateTime, Callable<Object>, Future> {

    private final static AtomicLong sequence = new AtomicLong(Long.MIN_VALUE);
    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final byte THREAD_POOL_SIZE = 4;

    private final ConcurrentNavigableMap<TimeAndOrderKey, Runnable> waitingRoom = new ConcurrentSkipListMap<>();
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        while (true) {
            TimeAndOrderKey firstKey = waitingRoom.firstKey();
            if (firstKey != null) {
                processNextTask(firstKey.startTime - System.currentTimeMillis());
            } else {
                waitForNextTask();
            }
        }
    }

    private void processNextTask(long timeToNextEvent) {
        if (timeToNextEvent <= 0) {
            toExecuteQueue(waitingRoom.pollFirstEntry().getValue());
        } else {
            try {
                sleep(timeToNextEvent);
            } catch (InterruptedException e) {
                logger.info("new firstKey");
            }
        }
    }

    private void waitForNextTask() {
        try {
            synchronized (waitingRoom) {
                waitingRoom.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future apply(DateTime dateTime, Callable<Object> callable) {
        return accept(dateTime.getMillis(), callable);
    }

    private Future accept(long time, Callable<Object> callable) {
        if (time < System.currentTimeMillis()) {
            return toExecuteQueue(new FutureTask<>(callable));
        } else {
            FutureTask x = new FutureTask<>(callable);
            toWaitingRoom(time, x);
            return x;
        }
    }

    private Future toWaitingRoom(long time, RunnableFuture runnable) {
        waitingRoom.put(new TimeAndOrderKey(time), runnable);
        if (time < waitingRoom.firstKey().startTime)
            interrupt();
        synchronized (waitingRoom) {
            waitingRoom.notify();
        }
        return runnable;
    }

    private Future toExecuteQueue(Runnable runnable) {
        return threadPool.submit(runnable);
    }

    private class TimeAndOrderKey implements Comparable<TimeAndOrderKey> {

        private final long startTime;
        private final long order;

        private TimeAndOrderKey(long startTime) {
            this.startTime = startTime;
            this.order = sequence.incrementAndGet();
        }

        @Override
        public int compareTo(TimeAndOrderKey o) {
            long diff = startTime - o.startTime;
            if (diff != 0)
                return Long.signum(diff);
            else
                return Long.signum(order - o.order);
        }

    }

}
