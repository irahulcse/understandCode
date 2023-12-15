package thesis.common.sources;

import thesis.common.GlobalConfig;
import thesis.policy.Policy;
import thesis.policy.SingleDataSectionPolicy;
import thesis.policy.SingleDataSectionSinglePriorityPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that creates policies from a CSV-input. It is used in the policy input source in each Flink job.
 * Example code snippet:
 * <pre>
 *     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *     env.fromElements(PolicyCreator.generatePolicy("dummyPolicy"))
 * </pre>
 * where a csv-file with the name "dummyPolicy" is parsed into an array of {@link Policy}.
 */
public class PolicyCreator {

    private static final String predicatePath = GlobalConfig.textInputSource + "/predicates/";
    private static final String petPath = GlobalConfig.textInputSource + "/petDescriptions/";
//    private static final Map<String, SingleDataSectionPolicy> dataSectionPolicyMap = new HashMap<>();
//    private static final Map<String, List<SingleDataSectionPolicy>> appPolicyMap = new HashMap<>();

    public static Policy[] generatePolicy(String policyCSVName) throws IOException {

        return readFromCSV(policyCSVName).toArray(new Policy[0]);
    }

    //
    public static SingleDataSectionSinglePriorityPolicy generateSinglePolicy(String predicateName, String petName, int priority) {
        String predicatePath = PolicyCreator.predicatePath + predicateName;
        String petPath = PolicyCreator.petPath + petName;
        String predicateFromFile, petFromFile;
        try {
            predicateFromFile = new String(Files.readAllBytes(Paths.get(predicatePath)));
        } catch (IOException e) {
            throw new RuntimeException("Invalid path of predicate: '" + predicateName + "' Cannot generate policy.");
        }
        try {
            petFromFile = new String(Files.readAllBytes(Paths.get(petPath)));
        } catch (IOException e) {
            throw new RuntimeException("Invalid path of PET: '" + petName + "' Cannot generate policy.");
        }
        return new SingleDataSectionSinglePriorityPolicy(priority, predicateFromFile, petFromFile);
    }

    public static List<Policy> readFromCSV(String fileName) throws IOException {
        Map<String, SingleDataSectionPolicy> dataSectionPolicyMap = new HashMap<>();
        Map<String, List<SingleDataSectionPolicy>> appPolicyMap = new HashMap<>();

        List<Policy> policies = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(GlobalConfig.textInputSource + "/" + fileName + ".csv"));
        List<SingleDataSectionPolicy> allDataSections = new ArrayList<>();

        for (int i = 1; i < lines.size(); i++) {
            String[] parts = lines.get(i).split(",");
            String name = parts[0];
            if (!appPolicyMap.containsKey(name)) {
                appPolicyMap.put(name, new ArrayList<>());
            }
            String dataSection = parts[1];
            if (!dataSectionPolicyMap.containsKey(dataSection)) {
                dataSectionPolicyMap.put(dataSection, new SingleDataSectionPolicy(dataSection, new ArrayList<>()));
            }
            String predicate = parts[2];
            String pet = parts[3];
            int prio = Integer.parseInt(parts[4]);
            dataSectionPolicyMap.get(dataSection).addAtomicPolicy(generateSinglePolicy(predicate, pet, prio));
            String nextName = null;
            if (i != lines.size() - 1) {
                nextName = lines.get(i + 1).split(",")[0];
            }
            if (!name.equals(nextName)) {
                for (Map.Entry<String, SingleDataSectionPolicy> entry : dataSectionPolicyMap.entrySet()) {
                    allDataSections.add(entry.getValue());
                }
                policies.add(new Policy(name, allDataSections));
                allDataSections.clear();
                dataSectionPolicyMap.clear();
            }
        }
        return policies;
    }
}
