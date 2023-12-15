package thesis.policy;

import org.json.JSONObject;

/**
 * The atomic definition of a privacy policy. It consists of a condition-action pair concerning only one data section and only one priority.
 */
public class SingleDataSectionSinglePriorityPolicy {

    public Integer priority;
    public String predicateAsString;
    public String petDescription;

    public SingleDataSectionSinglePriorityPolicy(Integer priority, String predicate, String petDescription) {
        this.priority = priority;
        this.predicateAsString = predicate;
        this.petDescription = petDescription;
    }

    protected JSONObject toJSON(){
        JSONObject o = new JSONObject();
        o.put("priority", priority);
        o.put("predicate", predicateAsString);
        o.put("pet", petDescription);
        return o;
    }

    @Override
    public String toString() {
        JSONObject o = toJSON();
        return o.toString();
    }
}
