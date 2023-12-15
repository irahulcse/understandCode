package thesis.common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import thesis.common.GlobalConfig;
import thesis.context.data.LocationData;

import java.awt.geom.Point2D;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LocationSource implements SourceFunction<LocationData> {
    private boolean isRunning = true;


    private final List<Point2D.Double> tranceList = readCSVSource();

    public LocationSource() throws IOException {

    }

    @Override
    public void run(SourceContext<LocationData> ctx) throws Exception {
        Thread.sleep(1000L);
        int token = 0;
        while (isRunning && token < tranceList.size()) {
            LocationData locationData = generateLocationRecord(token);
            ctx.collect(locationData);
            token++;
            // System.out.println("Location source: generated " + locationData);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private LocationData generateLocationRecord(int token) {
        Point2D.Double point = tranceList.get(token);
        return new LocationData(point);
    }

    private List<Point2D.Double> readCSVSource() throws IOException {
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

    public static double getX(String line) {
        return Double.parseDouble(line.split(",")[0].trim());
    }

    public static double getY(String line) {
        return Double.parseDouble(line.split(",")[1].trim());
    }

    public static int getTime(String line) {
        return Integer.parseInt(line.split(",")[2].trim());
    }

    public void outputTraceToCSV() throws IOException {
        FileWriter fileWriter = new FileWriter(GlobalConfig.textInputSource + "/generatedLocationPath.csv");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        int i = 0;
        bufferedWriter.write("x,y,time");
        bufferedWriter.newLine();
        for (Point2D.Double d : tranceList) {
            String outputLine = d.x + "," + d.y + "," + i;
            bufferedWriter.write(outputLine);
            bufferedWriter.newLine();
            i++;
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }


    public static void main(String[] args) throws IOException {

        LocationSource lc = new LocationSource();
        lc.outputTraceToCSV();
    }
}
