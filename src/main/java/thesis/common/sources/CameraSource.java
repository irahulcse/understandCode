package thesis.common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import thesis.common.GlobalConfig;
import thesis.context.data.ImageData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Timer;
import java.util.TimerTask;

public class CameraSource implements SourceFunction<ImageData> {
    private boolean isRunning = true;
    private final int recordNumber = 1105;
    private static final String path = GlobalConfig.imageSourcePath;
    private Timer timer;
    private final int frequency;
    private int eventCounter;
    private final int endSequenceNumber;

    public CameraSource(int frequency, int endSequenceNumber) {
        this.frequency = frequency;
        if (endSequenceNumber < recordNumber) {
            this.endSequenceNumber = endSequenceNumber;
        }else{
            this.endSequenceNumber = recordNumber;
        }
    }

    @Override
    public void run(SourceContext<ImageData> ctx) throws Exception {
        timer = new Timer();
        while(isRunning) {
            // Timer task is more reliable than Thread.sleep
            // Write inside the while loop to prevent the source from being closed.
            timer.scheduleAtFixedRate(new GenerateData(ctx), 1500, 1000 / frequency);
            Thread.sleep(100000); // Keep open for 100s
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        timer.cancel();
    }

    private class GenerateData extends TimerTask {
        private final SourceContext<ImageData> ctx;

        public GenerateData(SourceContext<ImageData> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (isRunning) { // Check if the source is still running
                if (eventCounter < endSequenceNumber) {
                    File file = new File(path + eventCounter + ".png");
                    ImageData data;
                    try {
                        data = new ImageData(Files.readAllBytes(file.toPath()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    data.setSequenceNumber(String.valueOf(eventCounter));
                    ctx.collect(data);
                    eventCounter++;
                } else {
                    cancel(); // Stop generating events when the condition is met
                }
            }
        }
    }
}
