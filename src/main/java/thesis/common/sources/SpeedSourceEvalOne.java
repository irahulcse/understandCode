package thesis.common.sources;

import com.mongodb.client.MongoCollection;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.Document;
import thesis.context.data.ScalarData;
import thesis.util.DBWrapper;

public class SpeedSourceEvalOne implements SourceFunction<ScalarData> {
    private boolean isRunning = true;
    private final String dataSectionName;
    private final int frequency;
    private final double endTime;


    public SpeedSourceEvalOne(String dataSectionName, int frequency, double endTime) {
        this.dataSectionName = dataSectionName;
        this.frequency = frequency;
        this.endTime = endTime;
    }

    @Override
    public void run(SourceContext<ScalarData> ctx) throws Exception {
        double time = 0;
        double increment = 1.0 / frequency;
        Thread.sleep(500L);
        while (isRunning && time <= endTime) {
            ScalarData scalarData = new ScalarData("speed", generateValue(time));
            ctx.collect(scalarData);
            DBWrapper.writeToDB(scalarData);
            time += increment;
            Thread.sleep(1000L / frequency);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private double generateValue(double token) {
        return 30 * Math.sin(Math.PI / 10 * token - Math.PI / 2) + 30;
    }

    private void writeToDB(ScalarData scalarData, MongoCollection<Document> collection) {
        Document document = scalarData.toMongoDBDocument();
        collection.insertOne(document);
    }
}
