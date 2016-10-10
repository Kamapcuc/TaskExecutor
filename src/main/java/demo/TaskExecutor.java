package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class TaskExecutor<T> extends Thread implements BiFunction<DateTime, Callable<T>, Future<T>>, Supplier<Runnable> {

    private final static AtomicLong sequence = new AtomicLong(Long.MIN_VALUE);
    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final byte THREAD_POOL_SIZE = 4;

    private final ConcurrentNavigableMap<TimeAndOrderKey, Runnable> waitingRoom = new ConcurrentSkipListMap<>();
    private final Queue<Runnable> executeQueue = new ConcurrentLinkedQueue<>();
    private final Collection<Thread> threadPool;

    public TaskExecutor() {
        super(new ThreadGroup("TaskExecutor"), "administrator");
        threadPool = constructThreadPool();
    }

    private Collection<Thread> constructThreadPool() {
        Collection<Thread> threadPool = new ArrayList<>();
        for (int i = 0; i < THREAD_POOL_SIZE; i++)
            threadPool.add(new InfiniteRunnableExecutor("executor-" + i, this));
        return Collections.unmodifiableCollection(threadPool);
    }

    @Override
    public synchronized void start() {
        this.threadPool.forEach(Thread::start);
        super.start();
    }

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
    public Future<T> apply(DateTime dateTime, Callable<T> callable) {
        if (executeQueue.size() > threadPool.size())
            logger.warn("Overload detected");
        FutureTask<T> result = new FutureTask<>(callable);
        accept(dateTime.getMillis(), result);
        return result;
    }

    private void accept(long time, Runnable runnable) {
        if (time < System.currentTimeMillis()) {
            toExecuteQueue(runnable);
        } else {
            toWaitingRoom(time, runnable);
        }
    }

    private void toWaitingRoom(long time, Runnable runnable) {
        waitingRoom.put(new TimeAndOrderKey(time), runnable);
        if (time < waitingRoom.firstKey().startTime)
            interrupt();
        synchronized (waitingRoom) {
            waitingRoom.notify();
        }
    }

    private void toExecuteQueue(Runnable runnable) {
        executeQueue.offer(runnable);
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public Runnable get() {
        return executeQueue.poll();
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
