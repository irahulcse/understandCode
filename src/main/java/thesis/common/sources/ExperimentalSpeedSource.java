package thesis.common.sources;

import com.mongodb.client.MongoCollection;
import thesis.jobs.CentralizedStatefulSpeedOnly;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.Document;
import thesis.context.data.ScalarData;

public class ExperimentalSpeedSource implements SourceFunction<ScalarData> {
    private boolean isRunning = true;
    private final String dataSectionName;
    private double[] nonlinearSpeedGeneration = {55.0, 58.0, 61.0, 65.0, 67.0, 69.0, 70.0, 71.0, 71.0, 72.0, 72.0, 72.0, 71.0, 70.0, 68.0, 66.0, 64.0, 63.0, 61.0, 58.0, 56.0};

    public ExperimentalSpeedSource(String dataSectionName) {
        this.dataSectionName = dataSectionName;

    }

    @Override
    public void run(SourceContext<ScalarData> ctx) throws Exception {
        MongoCollection<Document> collection = CentralizedStatefulSpeedOnly.dbWrapper.getDb().getCollection(dataSectionName);
        int token = 0;
        Thread.sleep(500L);
        System.out.println("Speed source: start generating records...");
        // For measurement
        // CentralizedStatefulSpeedOnly.outputTool.setStartTime(System.currentTimeMillis());
        //
        while (isRunning) {
            ScalarData scalarData = generateSpeedRecord(token);
            ctx.collect(scalarData);
            writeToDB(scalarData,collection);
            token++;
            //System.out.println("Speed source: generated " + scalarData);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private ScalarData generateSpeedRecord(int token) {
        double speed;
        if (token < 10) {
            speed = 5D * token;
            return new ScalarData("speed", speed);
        } else {
            try {
                speed = nonlinearSpeedGeneration[token - 10];
            } catch (IndexOutOfBoundsException e) {
                speed = 40;
                isRunning = false;
                System.out.println("Speed source: stopped generating records.");
            }
        }
        return new ScalarData("speed", speed);
    }

    private void writeToDB(ScalarData scalarData, MongoCollection<Document> collection) {
        Document document = scalarData.toMongoDBDocument();
        collection.insertOne(document);
    }
}
