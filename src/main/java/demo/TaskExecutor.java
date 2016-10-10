package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
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
            threadPool.add(new RunnableExecutor("executor-" + i, this));
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
            if (waitingRoom.isEmpty()) {
                waitForNextTask();
            } else {
                processNextTask();
            }
        }
    }

    private void processNextTask() {
        Map.Entry<TimeAndOrderKey, Runnable> firstEntry = waitingRoom.pollFirstEntry();
        long timeToNextEvent = firstEntry.getKey().startTime - System.currentTimeMillis();
        if (timeToNextEvent <= 0) {
            toExecuteQueue(firstEntry.getValue());
        } else {
            try {
                sleep(timeToNextEvent);
            } catch (InterruptedException e) {
                logger.info("Woke up - new first entry");
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
        if (time < waitingRoom.firstKey().startTime) {
            interrupt();
        }
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

    @Override
    protected void finalize() throws Throwable {
        threadPool.forEach(Thread::interrupt);
        super.finalize();
    }

    private class TimeAndOrderKey implements Comparable<TimeAndOrderKey> {

        private final long startTime;
        private final long order;

        private TimeAndOrderKey(long startTime) {
            this.startTime = startTime;
            this.order = sequence.getAndIncrement();
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
