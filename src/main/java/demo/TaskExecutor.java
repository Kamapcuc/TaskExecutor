package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class TaskExecutor<T> extends Thread implements BiFunction<DateTime, Callable<T>, Future<T>>, Supplier<Runnable> {

    private final static AtomicLong sequence = new AtomicLong(Long.MIN_VALUE);
    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final byte THREAD_POOL_SIZE = 3;

    private final ConcurrentNavigableMap<TimeAndOrderKey, Runnable> waitingRoom = new ConcurrentSkipListMap<>();
    private final Queue<Runnable> executeQueue = new ConcurrentLinkedQueue<>();
    private final Collection<Thread> threadPool = constructThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(true);

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
        while (running.get()) {
            if (waitingRoom.isEmpty()) {
                waitForTasks();
            } else {
                processNextTask();
            }
        }
    }

    private void waitForTasks() {
        try {
            logger.info("No tasks, waiting");
            synchronized (running) {
                running.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForTasks(long delay) {
        try {
            logger.info(String.format("Waiting for next for %d ms", delay));
            NotifyThread z = new NotifyThread(delay);
            synchronized (running) {
                z.start();
                running.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private void wakeUp() {
        synchronized (running) {
            running.notify();
        }
    }

    private void processNextTask() {
        long timeToNextEvent = waitingRoom.firstKey().startTime - System.currentTimeMillis();
        if (timeToNextEvent > 0) {
            waitForTasks(timeToNextEvent);
        } else {
            toExecuteQueue(waitingRoom.pollFirstEntry().getValue()); //it's ok if it's another
        }
    }

    @Override
    public Future<T> apply(DateTime dateTime, Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        accept(dateTime.getMillis(), future);
        return future;
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
        wakeUp();
        logger.info(String.format("Accepted task scheduled at %d", time));
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

    public void safeStop() {
        running.set(false);
        wakeUp();
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
            long diff = o.startTime - startTime;
            if (diff != 0)
                return Long.signum(diff);
            else
                return Long.signum(order - o.order);
        }

    }

    private class NotifyThread extends Thread {

        private final long delay;

        private NotifyThread(long delay) {
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                sleep(delay);
//                wakeUp();
                synchronized (running) {
                    running.notify();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
