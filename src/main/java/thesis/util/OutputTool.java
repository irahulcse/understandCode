package thesis.util;

import thesis.common.GlobalConfig;
import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.flink.SwitchingDecision;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A helper class intended for evaluation.
 * It helps to
 * <ul>
 *     <li>output the latency measurement in format in the console</li>
 *     <li>write a csv-file for further automatic analysis</li>
 * </ul>
 */
public class OutputTool {

    private static final String dataFormat = "|%8s|%14s|%8s|%8s|%8s|%8s|%8s|%8s|%8s|%n";
    private static final String switchFormat = "|%8s|%8s|%8s|%14s|%8s|%8s|%8s|%8s|%8s|%8s|%n";
    private static final String memoryFormat = "|%14s|%14s|%14s|%14s|%n";
    private static BufferedWriter bufferedSpeedWriter, bufferedSwitchWriter, bufferedMemoryWriter;
    private static final ConcurrentHashMap<String, Boolean> titleOutput = new ConcurrentHashMap<>();

    public OutputTool() throws IOException {
        String timeStamp = new SimpleDateFormat("yyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        Path folder = Files.createDirectories(Paths.get(GlobalConfig.csvOutputPath + timeStamp));
        FileWriter speedFileWriter = new FileWriter(folder.toString() + "/speed.csv");
        FileWriter switchFileWriter = new FileWriter(folder + "/switch.csv");
        FileWriter memoryFileWriter = new FileWriter(folder + "/memory.csv");
        bufferedSpeedWriter = new BufferedWriter(speedFileWriter);
        bufferedSwitchWriter = new BufferedWriter(switchFileWriter);
        bufferedMemoryWriter = new BufferedWriter(memoryFileWriter);
        titleOutput.put("speed", Boolean.TRUE);
        titleOutput.put("image", Boolean.TRUE);
        titleOutput.put("switch", Boolean.TRUE);
        titleOutput.put("memory", Boolean.TRUE);
    }

    /**
     * Formatted log for each object in interest, such as the latency of a {@link Data} record or a switching process
     *
     * @param object
     * @throws IOException
     */
    public static void log(Object object, String source) throws IOException {

        if (object instanceof Data<?>) {
            Data<?> cast = (Data<?>) object;

            title(object, cast.getDataSection());

            List<Object> items;
            if (cast instanceof ImageData) {
                items = List.of(cast.dataSection,
                        cast.getGenerateTime(),
                        ((ImageData) cast).getSequenceNumber(),
                        "-",
                        cast.getEvaluationLatency(),
                        cast.getTransmissionToProcess(),
                        cast.getProcessingLatency(),
                        cast.getPresentationLatency(),
                        cast.getE2ELatency());
            } else {
                items = List.of(cast.dataSection,
                        cast.getGenerateTime(),
                        cast.getData().toString(),
                        cast.getProcessedData().toString(),
                        cast.getEvaluationLatency(),
                        cast.getTransmissionToProcess(),
                        cast.getProcessingLatency(),
                        cast.getPresentationLatency(),
                        cast.getE2ELatency());
            }
            output(bufferedSpeedWriter, dataFormat, items);
        }
        if (object instanceof SwitchingDecision) {

            title(object, "switch");

            List<String> sections = ((SwitchingDecision) object).dataSections;
            for (String section : sections) {
                List<Object> items = List.of("switch",
                        section,
                        ((SwitchingDecision) object).decisionID.toString().split("-")[0],
                        ((SwitchingDecision) object).getGenerationTime(), // for plot in the figure of data delay
                        ((SwitchingDecision) object).getNetworkDelay(),
                        ((SwitchingDecision) object).getInstantiationTimeConsumption(),
                        ((SwitchingDecision) object).getSyncDelay(),
                        ((SwitchingDecision) object).getSwitchingTimeConsumption(),
                        ((SwitchingDecision) object).getTotalTimeConsumption(),
                        source);
                output(bufferedSwitchWriter, switchFormat, items);
            }
        }
        if (object instanceof MemoryUsage) {
            title(object, "memory");
            List<Object> items = List.of(((MemoryUsage) object).getTime(),
                    ((MemoryUsage) object).getMaxMem(),
                    ((MemoryUsage) object).getTotalMem(),
                    ((MemoryUsage) object).getUsedMem());
            output(bufferedMemoryWriter, memoryFormat, items);
        }
    }

    /**
     * Output the information to the console in a formatted manner and write to a csv-file.
     */
    private static void output(BufferedWriter writer, String format, List<Object> items) throws IOException {
        // Write to file
        StringBuilder outputString = new StringBuilder();
        for (Object s : items) {
            outputString.append(s.toString()).append(",");
        }
        outputString.deleteCharAt(outputString.length() - 1);

        writer.write(outputString.toString());
        writer.newLine();
        // Output to console.
        System.out.format(format, items.toArray());

    }

    public synchronized static void title(Object object, String type) throws IOException {
        if (titleOutput.get(type)) {
            if (object instanceof Data) {
                List<Object> titles = List.of("Type", "Time", "Orig.", "Ret", "Eval.", "Trans.", "Proc.", "Pres.", "E2E");
                output(bufferedSpeedWriter, dataFormat, titles);
            }
            if (object instanceof SwitchingDecision) {
                List<Object> titles = List.of("Type", "Section", "ID", "Issue", "Net.", "Inst.", "Sync.", "Switch", "Total", "From");
                output(bufferedSwitchWriter, switchFormat, titles);
            }
            if (object instanceof MemoryUsage) {
                List<Object> titles = List.of("Time", "Max", "Total", "Used");
                output(bufferedMemoryWriter, memoryFormat, titles);
            }
            titleOutput.put(type, Boolean.FALSE);
        }
    }

    /**
     * This method returns a thread that should be used in a shutdown hook.
     * It closes the {@code BufferedWriter}, performs the automatic analysis and store the result in the desired folder.
     *
     * @throws IOException
     */
    public Thread complete(boolean runAnalysis) {
        Runnable runnable = () -> {
            try {

                bufferedSpeedWriter.close();
                bufferedSwitchWriter.close();
                bufferedMemoryWriter.close();
                if (runAnalysis) {
                    Runtime.getRuntime().exec("%s %s".formatted(GlobalConfig.pythonRuntime, GlobalConfig.analyzerPath));
                    System.out.println("Analysis results are written.");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        };
        return new Thread(runnable);
    }

}
