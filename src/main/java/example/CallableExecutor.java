package example;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class CallableExecutor extends Thread {

    private static final Logger logger = Logger.getLogger(CallableExecutor.class);

    private final Supplier<Callable> supplier;

    public CallableExecutor(String name, Supplier<Callable> supplier) {
        super(name);
        this.supplier = supplier;
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        while (true) {
            Callable target = supplier.get();
            if (target != null)
                execute(target);
            else
                waitForNextTask();
        }
    }

    private void execute(Callable target) {
        try {
            target.call();
        } catch (Exception e) {
            logger.error("Exception during task execution", e);
        }
    }

    private void waitForNextTask() {
        try {
            synchronized (supplier) {
                supplier.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
