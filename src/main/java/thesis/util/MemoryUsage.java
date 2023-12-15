package thesis.util;

public class MemoryUsage {

    private final long time, maxMem, totalMem, usedMem;

    public MemoryUsage(long time, long maxMem, long totalMem, long freeMem) {
        this.time = time;
        this.maxMem = maxMem;
        this.totalMem = totalMem;
        this.usedMem = totalMem - freeMem;
    }

    public long getTime() {
        return time;
    }

    public String getMaxMem() {
        return String.valueOf(maxMem);
    }

    public String getTotalMem() {
        return String.valueOf(totalMem);
    }

    public String getUsedMem() {
        return String.valueOf(usedMem);
    }
}
