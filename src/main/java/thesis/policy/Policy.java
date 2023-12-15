package thesis.policy;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * This class describes the privacy policy for a certain application.
 * The name of the application is the given in the field {@link Policy#appName}.
 * The policies for each involved data sections are stored in a list under {@link Policy#dataSectionPoliciesWithPriority}
 */
public class Policy {

    private final String appName;

    private final List<SingleDataSectionPolicy> dataSectionPoliciesWithPriority = new ArrayList<>();

    public Policy(String appName, Collection<SingleDataSectionPolicy> dataSectionPoliciesWithPriority) {
        this.appName = appName;
        this.dataSectionPoliciesWithPriority.addAll(dataSectionPoliciesWithPriority);
    }

    /**
     * Returns the individual policy for the desired data section
     * @param dataSection the name of the data section
     * @return the individual policy
     */
    public SingleDataSectionPolicy getDataSectionPolicies(String dataSection) {
        for (SingleDataSectionPolicy s : dataSectionPoliciesWithPriority) {
            if (s.dataSection.equals(dataSection)) {
                return s;
            }
        }
        throw new IllegalArgumentException(dataSection + " is not defined in the current policy");
    }

    /**
     *
     * @return A set of the data sections involved in this policy
     */
    public Set<String> getAllDataSections() {
        Set<String> dataSectionSet = new HashSet<>();
        for (SingleDataSectionPolicy s : dataSectionPoliciesWithPriority){
            dataSectionSet.add(s.dataSection);
        }
        return dataSectionSet;
    }

    public String getAppName() {
        return appName;
    }

    @Override
    public String toString() {
        JSONObject o = new JSONObject();
        o.put("appName", appName);
        JSONArray a = new JSONArray();
        for (SingleDataSectionPolicy sp: dataSectionPoliciesWithPriority){
            a.put(sp.toJSON());
        }
        o.put("policy", a);
        return o.toString();
    }
}
