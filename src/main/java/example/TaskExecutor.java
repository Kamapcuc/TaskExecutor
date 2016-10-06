package example;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class TaskExecutor extends Thread implements BiConsumer<DateTime, Callable>, Supplier<Callable> {

    private static final Logger logger = Logger.getLogger(TaskExecutor.class);
    private static final byte THREAD_POOL_SIZE = 4;

    private final ConcurrentNavigableMap<Long, List<Callable>> waitingRoom = new ConcurrentSkipListMap<>();
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
            long timeToNextEvent = waitingRoom.firstKey() - System.currentTimeMillis();
            if (timeToNextEvent <= 0) {
                Map.Entry<Long, List<Callable>> firstEntry = waitingRoom.pollFirstEntry();
                firstEntry.getValue().forEach(executeQueue::offer);
            } else {
                try {
                    sleep(timeToNextEvent);
                } catch (InterruptedException e) {
                    logger.info("new firstKey");
                }
            }
        }
    }

    @Override
    public Callable get() {
        return executeQueue.poll();
    }

    private void toWaitingRoom(long time, Callable callable) {
        List<Callable> container = Collections.synchronizedList(Arrays.asList(callable));
        List<Callable> putByOtherThread = waitingRoom.putIfAbsent(time, container);
        if (putByOtherThread != null)
            container = putByOtherThread;
        container.add(callable);
        if (!waitingRoom.containsKey(time))
            executeQueue.offer(callable);
    }

    @Override
    public void accept(DateTime dateTime, Callable callable) {
        if (executeQueue.size() > threadPool.size())
            logger.warn("Overload detected");
        accept(dateTime.getMillis(), callable);
    }

    private void accept(long time, Callable callable) {
        if (time > System.currentTimeMillis()) {
            executeQueue.offer(callable);
        } else {
            toWaitingRoom(time, callable);
            if (time < waitingRoom.firstKey() && getState() == Thread.State.TIMED_WAITING)
                interrupt();
        }
    }

}
