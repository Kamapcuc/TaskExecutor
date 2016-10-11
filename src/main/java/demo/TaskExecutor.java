package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
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
    public void run() {
        while (running.get()) {
            if (waitingRoom.isEmpty()) {
                emptyWait();
            } else {
                processNextTask();
            }
        }
    }

    private void emptyWait() {
        logger.info("No tasks, waiting");
        waitForTasks(null);
    }

    private void waitForTasks(NotifyThread wakeUpThread) {
        try {
            synchronized (running) {
                if (wakeUpThread != null)
                    wakeUpThread.start();
                running.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForTasks(long delay) {
        logger.info(String.format("Waiting %d ms for next task", delay));
        NotifyThread notifyThread = new NotifyThread(delay);
        waitForTasks(notifyThread);
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
            long diff = startTime - o.startTime;
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
                logger.info(String.format("%d ms delay thread waked up", delay));
                wakeUp();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
