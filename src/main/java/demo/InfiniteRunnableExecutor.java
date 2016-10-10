package demo;

import java.util.function.Supplier;

public class InfiniteRunnableExecutor extends Thread {

    private final Supplier<Runnable> supplier;

    public InfiniteRunnableExecutor(String name, Supplier<Runnable> supplier) {
        super(name);
        this.supplier = supplier;
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        while (true) {
            Runnable target = supplier.get();
            if (target != null)
                target.run();
            else
                waitForNextTask();
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
