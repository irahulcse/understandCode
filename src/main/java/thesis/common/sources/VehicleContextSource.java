package thesis.common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class VehicleContextSource implements SourceFunction<VehicleContext> {
    private boolean isRunning = true;
    private final List<Point2D.Double> tranceList = readLocationCSV();
    private final List<Double> speedData = generateSpeedRawData();
    private int imageFrequency;
    private static final String path = GlobalConfig.imageSourcePath;

    public VehicleContextSource(int imageFrequency) throws IOException {
        this.imageFrequency = imageFrequency;

    }

    @Override
    public void run(SourceContext<VehicleContext> ctx) throws Exception {
        int index = 0;
        VehicleContext vc = new VehicleContext();
        while (isRunning) {
            if (index % 10 == 0) {
                LocationData lc = new LocationData(tranceList.get(index / 10));
                ScalarData sd = new ScalarData("speed", speedData.get(index/10));
                vc.update(lc);
                vc.update(sd);
            }
            ImageData im = getImage(index);
            vc.update(im);
            ctx.collect(vc);
            index++;
            Thread.sleep(1000L / imageFrequency);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private List<Double> generateSpeedRawData() throws IOException {
        List<Integer> intervals = new ArrayList<>();
        List<Double> calculatedSpeed = new ArrayList<>();
        List<Double> speedTrace = new ArrayList<>();

        calculatedSpeed.add(0d); // The first record is 0.
        intervals.add(0);
        List<String> locationRecords = Files.readAllLines(Paths.get(GlobalConfig.textInputSource + "/locationSource.csv"));
        for (int i = 1; i < locationRecords.size() - 1; i++) {
            String thisLine = locationRecords.get(i);
            String nextLine = locationRecords.get(i + 1);
            double thisX = LocationSource.getX(thisLine);
            double thisY = LocationSource.getY(thisLine);
            double nextX = LocationSource.getX(nextLine);
            double nextY = LocationSource.getY(nextLine);
            int interval = LocationSource.getTime(nextLine);
            intervals.add(interval);
            double distance = Math.sqrt(Math.pow(thisX - nextX, 2) + Math.pow(thisY - nextY, 2));
            calculatedSpeed.add(distance / interval * 3.6); // Speed has the unit km/h
            if (i == calculatedSpeed.size() - 2) {
                calculatedSpeed.add(0d);
            }
        }
        for (int index = 0; index < calculatedSpeed.size() - 1; index++) {
            double startSpeed = calculatedSpeed.get(index);
            double endSpeed = calculatedSpeed.get(index + 1);
            double interval = intervals.get(index + 1);

            speedTrace.add(startSpeed);
            for (int j = 1; j < interval; j++) {
                speedTrace.add(startSpeed + (endSpeed - startSpeed) / interval * j);
            }
            if (index == calculatedSpeed.size()-2){
                speedTrace.add(endSpeed);
            }
        }
        return speedTrace;
    }

    private ImageData getImage(int sequenceNumber) throws IOException {
        File file = new File(path + sequenceNumber + ".png");
        ImageData data = new ImageData(Files.readAllBytes(file.toPath()));
        data.setSequenceNumber(String.valueOf(sequenceNumber));
        return data;
    }

    // Location Data
    private List<Point2D.Double> readLocationCSV() throws IOException {
        List<Point2D.Double> trace = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(GlobalConfig.textInputSource + "/locationSource.csv"));
        for (int i = 1; i < lines.size() - 1; i++) {
            String thisLine = lines.get(i);
            String nextLine = lines.get(i + 1);

            double thisX = getX(thisLine);
            double thisY = getY(thisLine);
            double nextX = getX(nextLine);
            double nextY = getY(nextLine);
            int numberOfIntermediate = getTime(nextLine);
            double incrementX = (nextX - thisX) / numberOfIntermediate;
            double incrementY = (nextY - thisY) / numberOfIntermediate;

            trace.add(new Point2D.Double(thisX, thisY));
            for (int j = 1; j < numberOfIntermediate; j++) {
                trace.add(new Point2D.Double(thisX + j * incrementX, thisY + j * incrementY));
            }
            if (i == lines.size() - 2) {
                trace.add(new Point2D.Double(nextX, nextY));
            }

        }
        return trace;
    }

    private double getX(String line) {
        return Double.parseDouble(line.split(",")[0].trim());
    }

    private double getY(String line) {
        return Double.parseDouble(line.split(",")[1].trim());
    }

    private int getTime(String line) {
        return Integer.parseInt(line.split(",")[2].trim());
    }
}
