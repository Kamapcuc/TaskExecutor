package demo;

import java.util.function.Supplier;

public class RunnableExecutor extends Thread {

    private final Supplier<Runnable> supplier;

    public RunnableExecutor(int num, Supplier<Runnable> runnableSupplier) {
        super("Executor-" + num);
        setDaemon(true);
        this.supplier = runnableSupplier;
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        while (true) {
            Runnable target = supplier.get();
            if (target != null) {
                target.run();
            } else {
                waitForNextTask();
            }
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
