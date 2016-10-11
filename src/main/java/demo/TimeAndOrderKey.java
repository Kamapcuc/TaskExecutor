package demo;

import java.util.concurrent.atomic.AtomicLong;

class TimeAndOrderKey implements Comparable<TimeAndOrderKey> {

    private final static AtomicLong sequence = new AtomicLong(Long.MIN_VALUE);

    private final long startTime;
    private final long order;

    TimeAndOrderKey(long startTime) {
        this.startTime = startTime;
        this.order = sequence.getAndIncrement(); // or System.currentTimeMillis()
    }

    @Override
    public int compareTo(TimeAndOrderKey o) {
        long diff = startTime - o.startTime;
        if (diff != 0)
            return Long.signum(diff);
        else
            return Long.signum(order - o.order);
    }

    public long getStartTime() {
        return startTime;
    }

}
