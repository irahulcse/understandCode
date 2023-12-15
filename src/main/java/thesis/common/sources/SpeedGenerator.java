package thesis.common.sources;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class SpeedGenerator {

    private final List<Double> speedFromLocation;
    private final List<Integer> intervals = new ArrayList<>();
    private final double acceleration = 2;

    private final List<Double> speedTrace;

    public SpeedGenerator() throws IOException {
        this.speedFromLocation = calculateSpeedFromCSV();
        this.speedTrace = generateSpeed();
    }

    public List<Double> generateSpeed() {
        List<Double> speedTrace = new ArrayList<>();


        for (int index = 0; index < speedFromLocation.size() - 1; index++) {
            double startSpeed = speedFromLocation.get(index);
            double endSpeed = speedFromLocation.get(index + 1);
            double interval = intervals.get(index + 1);

            speedTrace.add(startSpeed);
            for (int j = 1; j < interval; j++) {
                speedTrace.add(startSpeed + (endSpeed - startSpeed) / interval * j);
            }
            if (index == speedFromLocation.size()-2){
                speedTrace.add(endSpeed);
            }
        }
        return speedTrace;
    }

    public void outputTraceToCSV() throws IOException {
        FileWriter fileWriter = new FileWriter("/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/textInputSources" + "/generatedSpeed.csv");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        int i = 0;
        bufferedWriter.write("v");
        bufferedWriter.newLine();
        for (Double d : speedTrace) {
            String outputLine = String.format("%.2f", d);
            bufferedWriter.write(outputLine);
            bufferedWriter.newLine();
            i++;
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }


    private List<Double> calculateSpeedFromCSV() throws IOException {
        List<Double> calculatedSpeed = new ArrayList<>();
        calculatedSpeed.add(0d); // The first record is 0.
        intervals.add(0);
        List<String> locationRecords = Files.readAllLines(Paths.get("/Users/zhudasch/Documents/Studium/05_Masterarbeit/LiveAdaptationFramework/textInputSources" + "/locationSource.csv"));
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
        return calculatedSpeed;
    }

    public static void main(String[] args) throws IOException {
        SpeedGenerator sg = new SpeedGenerator();
        sg.outputTraceToCSV();
    }

}
