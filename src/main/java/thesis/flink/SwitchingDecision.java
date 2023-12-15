package thesis.flink;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * A class contains the information about the upcoming switching event.
 * It will be broadcast to the PET Enforcement operator in the downstream.
 * The contents are:
 * <li> the name of the data section</li>
 * <li> the description of the next PET as string</li>
 * <li> the timestamp of the data record triggering this switch</li>
 * <li> the UUID of this switching event</li>
 */
public class SwitchingDecision {

    public List<String> dataSections;
    public String petDescriptionString;
    public Long generationTime;
    public UUID decisionID;
    // for measurement
    private String evaluationMode;
    public static String BENCHMARK = "BENCHMARK";
    public static String CENTRALIZED = "CENTRALIZED";
    public static String DISTRIBUTED = "DISTRIBUTED";
    private Long instantiationBegin; // When it enters the PET enforcement operator
    private Long instantiationEnd; // When the new PET is built
    private Long switchBegin; // When the switch condition is met and the transition starts
    private Long switchEnd; // When the transition ends
    private Long syncEnd; // For parallel processed PETs, when all parallel instances are ready

    public SwitchingDecision(Collection<String> dataSection, String petDescriptionString, Long generationTime, UUID decisionID) {
        this.dataSections = new ArrayList<>(dataSection);
        this.petDescriptionString = petDescriptionString;
        this.generationTime = generationTime;
        this.decisionID = decisionID;
    }

    @Override
    public String toString() {
        JSONObject object = new JSONObject(petDescriptionString);
        return "[" + dataSections + ", generated @" + generationTime + ", PET: " + object.getString("name") + ", switch ID: " + decisionID + "]";
    }

    private String get(Long time) {
        if (time == null) return "-";
        return String.valueOf(time);
    }

    public void setEvaluationMode(String mode) {
        this.evaluationMode = mode;
    }

    public String getInstantiationBegin() {
        return get(instantiationBegin);
    }

    public void setInstantiationBegin(Long instantiationBegin) {
        this.instantiationBegin = instantiationBegin;
    }

    public String getSwitchBegin() {
        return get(switchBegin);
    }

    public String getGenerationTime() {
        return get(generationTime);
    }

    public void setSwitchBegin(Long switchBegin) {
        this.switchBegin = switchBegin;
    }

    public String getSwitchEnd() {
        return get(switchEnd);
    }

    public void setSwitchEnd(Long switchEnd) {
        this.switchEnd = switchEnd;
    }


    public void setSyncEnd(Long syncEnd) {
        this.syncEnd = syncEnd;
    }

    public String getInstantiationEnd() {
        return get(instantiationEnd);
    }

    public void setInstantiationEnd(Long instantiationEnd) {
        this.instantiationEnd = instantiationEnd;
    }

    public String getNetworkDelay() {
        if (instantiationBegin == null) return "-";
        return String.valueOf(instantiationBegin - generationTime);
    }

    public String getInstantiationTimeConsumption() {
        if (instantiationEnd == null) return "-";
        return String.valueOf(instantiationEnd - instantiationBegin);
    }

    public String getSyncDelay() {
        if (syncEnd == null) return "-";
        return String.valueOf(syncEnd - instantiationEnd);
    }

    public String getSwitchingTimeConsumption() {
        if (switchEnd == null || switchBegin == null) return "-";
        return String.valueOf(switchEnd - switchBegin);
    }

    public String getTotalTimeConsumption() {
        switch (evaluationMode) {
            case "BENCHMARK" -> {
                return String.valueOf(switchEnd - generationTime);
            }
            case "CENTRALIZED" -> {
                return String.valueOf(Long.parseLong(getNetworkDelay()) + Long.parseLong(getInstantiationTimeConsumption()) + Long.parseLong(getSwitchingTimeConsumption()));
            }
            case "DISTRIBUTED" -> {
                if (syncEnd==null) return "-"; // refer to thesis. Total switching time approx. equals to syncEnd-generationTime. The value of syncEnd is only available in the output from feedback processor
                else return String.valueOf(syncEnd-generationTime);
            }
            default -> throw new IllegalArgumentException("Cannot handle the evaluation mode: " + evaluationMode);
        }
    }
}
