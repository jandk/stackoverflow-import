package be.twofold.stackoverflow.util;

public final class ProgressMonitor {

    private static final long TimeoutNanos = 1_000_000_000;

    private long lastUpdate;
    private int lastCount;
    private int count;

    public ProgressMonitor() {
        initialize();
    }

    public void initialize() {
        lastUpdate = System.nanoTime();
    }

    public int incrementCount() {
        count++;
        long nanoTime = System.nanoTime();
        if (nanoTime - lastUpdate >= TimeoutNanos) {
            print();
            lastCount = count;
            lastUpdate = nanoTime;
        }
        return count;
    }

    public void print() {
        System.out.println("Saved " + count + " items (" + (count - lastCount) + " items/s)");
    }

}
