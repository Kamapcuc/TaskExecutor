package example;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class TaskExecutor extends Thread implements BiConsumer<DateTime, Callable>, Supplier<Callable> {

    private final static AtomicLong sequence = new AtomicLong(Long.MIN_VALUE);
    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final byte THREAD_POOL_SIZE = 4;

    private final ConcurrentNavigableMap<TimeAndOrderKey, Callable> waitingRoom = new ConcurrentSkipListMap<>();
    private final Queue<Callable> executeQueue = new ConcurrentLinkedQueue<>();
    private final Collection<Thread> threadPool;

    public TaskExecutor() {
        super(new ThreadGroup("TaskExecutor"), "administrator");
        threadPool = constructThreadPool();
    }

    private Collection<Thread> constructThreadPool() {
        Collection<Thread> threadPool = new ArrayList<>();
        for (int i = 0; i < THREAD_POOL_SIZE; i++)
            threadPool.add(new CallableExecutor("executor-" + i, this));
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
    public void accept(DateTime dateTime, Callable callable) {
        if (executeQueue.size() > threadPool.size())
            logger.warn("Overload detected");
        accept(dateTime.getMillis(), callable);
    }

    private void accept(long time, Callable callable) {
        if (time < System.currentTimeMillis()) {
            toExecuteQueue(callable);
        } else {
            toWaitingRoom(time, callable);
        }
    }

    private void toWaitingRoom(long time, Callable callable) {
        waitingRoom.put(new TimeAndOrderKey(time), callable);
        if (time < waitingRoom.firstKey().startTime)
            interrupt();
        synchronized (waitingRoom) {
            waitingRoom.notify();
        }
    }

    private void toExecuteQueue(Callable callable) {
        executeQueue.offer(callable);
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public Callable get() {
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
