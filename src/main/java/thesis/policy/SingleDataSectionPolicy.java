package thesis.policy;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 *  The policy for each data section in {@link Policy}.
 *  The contained atomic policy definitions {@link SingleDataSectionSinglePriorityPolicy} are ordered by the ascending order of the priority value.
 */
public class SingleDataSectionPolicy {

    public String dataSection;

    public PriorityQueue<SingleDataSectionSinglePriorityPolicy> pq;

    public SingleDataSectionPolicy(String dataSection, PriorityQueue<SingleDataSectionSinglePriorityPolicy> pq) {
        this.dataSection = dataSection;
        this.pq = pq;
    }

    public SingleDataSectionPolicy(String dataSection, Collection<SingleDataSectionSinglePriorityPolicy> singlePolicies) {
        this.dataSection = dataSection;
        this.pq = new PriorityQueue<>(new SinglePolicyComparator());
        pq.addAll(singlePolicies);
    }

    protected JSONObject toJSON(){
        JSONObject o = new JSONObject();
        o.put("dataSection", dataSection);
        JSONArray a = new JSONArray();
        for (SingleDataSectionSinglePriorityPolicy sp: pq){
            a.put(sp.toJSON());
        }
        o.put("rule", a);
        return o;
    }

    public void addAtomicPolicy(SingleDataSectionSinglePriorityPolicy spp){
        pq.add(spp);
    }

    @Override
    public String toString() {
        JSONObject o = toJSON();
        return o.toString();
    }

    static class SinglePolicyComparator implements Comparator<SingleDataSectionSinglePriorityPolicy>{
        @Override
        public int compare(SingleDataSectionSinglePriorityPolicy o1, SingleDataSectionSinglePriorityPolicy o2) {
            return Integer.compare(o2.priority, o1.priority);
        }
    }
}
