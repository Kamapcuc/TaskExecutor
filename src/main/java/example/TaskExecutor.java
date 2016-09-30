package example;

import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TaskExecutor extends Thread implements Consumer<Pair<Date, Callable>>, Supplier<Callable> {

    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final Date BEGINNING_OF_TIME = new Date(0);
    private static final Date INFINITE_FUTURE = new Date(Long.MAX_VALUE);
    private static final byte THREAD_POOL_SIZE = 4;

    private volatile SortedMap<Date, Callable> waitingRoom = new TreeMap<>();

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
    public void run() {
        long now = System.currentTimeMillis();
        Date today = new Date(System.currentTimeMillis());
        SortedMap<Date, Callable> past = waitingRoom.subMap(BEGINNING_OF_TIME, today);
        SortedMap<Date, Callable> future = waitingRoom.subMap(today, INFINITE_FUTURE);
        waitingRoom = future;
        past.values().forEach(executeQueue::offer);
        try {
            Thread.sleep(future.firstKey().getTime() - now);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void start() {
        this.threadPool.forEach(Thread::start);
        super.start();
    }

    @Override
    public void accept(Pair<Date, Callable> task) {
        synchronized (this) {
            if (executeQueue.size() > threadPool.size())
                logger.warn("Overload detected");
            waitingRoom.put(task.getKey(), task.getValue());
        }
    }

    @Override
    public Callable get() {
        return executeQueue.poll();
    }

}
