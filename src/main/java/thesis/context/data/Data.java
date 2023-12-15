package thesis.context.data;

import org.bson.Document;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base class for sensor readings.
 * @param <T> The class of the raw sensor reading. For example, scalar value - {@link Double}
 */
public class Data<T> implements Serializable {

    public String dataSection;
    T data;
    T processedData;
    int key;
    Long generateTime;
    UUID triggerID = null;
    // ------------------------------------- For Evaluation -------------------------------------
    Long evaluationTime;
    Long processTime;
    Long processBegin;
    Long processEnd;
    Long presentationTime;

    public void setEvaluationTime(Long evaluationTime) {
        this.evaluationTime = evaluationTime;
    }

    public void setPresentationTime(Long presentationTime) {
        this.presentationTime = presentationTime;
    }
// ------------------------------------------------------------------------------------------

    public Data(String dataSection, T data, long generateTime) {
        this.dataSection = dataSection;
        this.data = data;
        this.generateTime = generateTime;
    }

    public boolean isTrigger() {
        return triggerID != null;
    }

    public long getGenerateTime() {
        return generateTime;
    }

    public void setTrigger(UUID uuid) {
        this.triggerID = uuid;
    }

    public UUID getTriggerID() {
        return triggerID;
    }

    public String getDataSection() {
        return dataSection;
    }

    public long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Long processTime) {
        this.processEnd = processTime;
    }

    public T getData() {
        return data;
    }

    public T getProcessedData() {
        return processedData;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public void setProcessedData(T processedData) {
        this.processedData = processedData;
    }

    /**
     * Transform this instance to a {@link Document} that can be written in MongoDB.
     *
     * @return An instance of {@link Document} to be written in MongoDB.
     */
    public Document toMongoDBDocument() {
        Document doc = new Document();
        doc.put("dataSection", dataSection);
        doc.put("data", data);
        doc.put("generationTime", generateTime);
        return doc;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Data<?>)) {
            return false;
        } else {
            return ((Data<?>) obj).dataSection.equals(dataSection) && ((Data<?>) obj).generateTime == getGenerateTime();
        }
    }

    // ------------------------------------- For Evaluation -------------------------------------
    public String getEvaluationLatency() {
        if (evaluationTime == null) return "-";
        return String.valueOf(evaluationTime - generateTime);
    }


    public String getPresentationLatency() {
        if (presentationTime == null) return "-";
        return String.valueOf(presentationTime - processEnd);
    }

    public void setProcessEnd(Long processEnd) {
        this.processEnd = processEnd;
    }

    public void setProcessBegin(Long processBegin) {
        this.processBegin = processBegin;
    }

    public String getProcessingLatency() {
        return interval(processBegin, processEnd);
    }

    public String getTransmissionToProcess() {
        return interval(evaluationTime, processBegin);

    }

    public String getE2ELatency() {
        if (presentationTime == null) return String.valueOf(processEnd - generateTime);
        return String.valueOf(presentationTime - generateTime);
    }

    private String interval(Long begin, Long end) {
        if (begin == null || end == null) return "-";
        Long result = end - begin;
/*        if (result == 0) {
            return "<1";
        }*/
        return String.valueOf(result);
    }
    // ------------------------------------------------------------------------------------------

}
