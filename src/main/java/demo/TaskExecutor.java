package demo;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class TaskExecutor<T> extends Thread implements BiFunction<DateTime, Callable<T>, Future<T>>, Supplier<Runnable> {

    private final static Logger logger = Logger.getLogger(TaskExecutor.class);
    private final static byte THREAD_POOL_SIZE = 3;

    private final ConcurrentNavigableMap<TimeAndOrderKey, Runnable> waitingRoom = new ConcurrentSkipListMap<>();
    private final Queue<Runnable> executeQueue = new ConcurrentLinkedQueue<>();
    private final Collection<Thread> threadPool = constructThreadPool();
    private final Object monitor = new Object();
    private volatile boolean running = true;
    private volatile long nextScheduledTime = Long.MAX_VALUE;

    private Collection<Thread> constructThreadPool() {
        Collection<Thread> threadPool = new ArrayList<>();
        for (int i = 0; i < THREAD_POOL_SIZE; i++)
            threadPool.add(new RunnableExecutor(i, this));
        return Collections.unmodifiableCollection(threadPool);
    }

    @Override
    public synchronized void start() {
        this.threadPool.forEach(Thread::start);
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            if (waitingRoom.isEmpty()) {
                emptyWait();
            } else {
                processNextTask();
            }
        }
    }

    private void emptyWait() {
        logger.info("No tasks, waiting");
        wait(null);
    }

    private void wakeUp() {
        synchronized (monitor) {
            monitor.notify();
        }
    }

    private void wait(NotifyThread wakeUpThread) {
        try {
            synchronized (monitor) {
                if (wakeUpThread != null)
                    wakeUpThread.start();
                monitor.wait();
                logger.info(String.format("Waked up at %d", System.currentTimeMillis()));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void delayTask(long delay) {
        logger.info(String.format("Waiting %d ms for next task", delay));
        nextScheduledTime = System.currentTimeMillis() + delay;
        NotifyThread notifyThread = new NotifyThread(delay);
        wait(notifyThread);
        if (System.currentTimeMillis() < nextScheduledTime)
            notifyThread.interrupt();
    }

    private void processNextTask() {
        long timeToNextEvent = waitingRoom.firstKey().getStartTime() - System.currentTimeMillis();
        if (timeToNextEvent > 0) {
            delayTask(timeToNextEvent);
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
        if (time < nextScheduledTime)
            wakeUp();
        logger.info(String.format("Accepted task scheduled to %d", time));
    }

    private void toExecuteQueue(Runnable runnable) {
        if (executeQueue.size() > threadPool.size())
            logger.warn("Overload detected");
        executeQueue.offer(runnable);
        notifyToThreadPool();
    }

    @Override
    public Runnable get() {
        return executeQueue.poll();
    }

    private void notifyToThreadPool() {
        synchronized (this) {
            notifyAll();
        }
    }

//    @Override
//    protected void finalize() throws Throwable {
//        threadPool.forEach(Thread::interrupt);
//        super.finalize();
//    }

    public void safeStop() {
        logger.info("Stopping");
        running = false;
        wakeUp();
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
                logger.info(String.format("Notify thread stops sleeping %d ms delay", delay));
                wakeUp();
            } catch (InterruptedException e) {
                logger.info(String.format("Notify thread for %d ms delay was interrupted", delay));
            }
        }

    }

}
