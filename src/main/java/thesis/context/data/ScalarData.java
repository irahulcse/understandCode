package thesis.context.data;

import org.json.JSONObject;

public class ScalarData extends Data<Double> {


    public ScalarData(String dataSection, Double data) {
        super(dataSection, data, System.currentTimeMillis());
    }

    public ScalarData(String dataSection, Double data, Long timestamp) {
        super(dataSection, data, timestamp);
    }

    @Override
    public String toString() {
        if (data == null) return this.dataSection + ": dummy data, Timestamp: " + generateTime;
        if (processedData == null && processTime == null)
            return this.dataSection + ": " + this.data;
        if (processedData == null)
            return this.dataSection + ": " + this.data + " unchanged ";
        return this.dataSection + ": " + this.data + " -> " + this.processedData;
    }

    public String originalDataToString() {
        return String.format("%.02f", getData()) + "km/h ";
    }

    public String processedDataToString() {
        if (processedData == null) return "-";
        return String.format("%.02f", getProcessedData()) + "km/h ";
    }

}
