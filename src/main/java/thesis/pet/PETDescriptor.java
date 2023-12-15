package thesis.pet;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;


/**
 * A class that describes the name, the types of input and output, as well as the parameters and their values.
 */
public class PETDescriptor {

    private final String name;
    private final List<StateWindow> stateWindowList = new ArrayList<>();
    private final List<String> inputDataSection = new ArrayList<>();
    private final List<String> outputDataSections = new ArrayList<>();
    private final Class<?>[] constructorClasses;
    private final Object[] parameterWithInitialValues;


    public PETDescriptor(String jsonString) throws ClassNotFoundException, JsonProcessingException {
        JSONObject o = new JSONObject(jsonString);
        this.name = o.getString("name");
        JSONArray windows = o.getJSONArray("stateWindow");
        windows.forEach(t -> stateWindowList.add(new StateWindow((JSONObject) t)));
        this.constructorClasses = readClasses(jsonString);
        this.parameterWithInitialValues = readInitializedParameters(jsonString);
        JSONArray in = o.getJSONArray("inputDataSection");
        in.forEach(t -> inputDataSection.add((String) t));
        JSONArray out = o.getJSONArray("outputDataSection");
        out.forEach(t -> outputDataSections.add((String) t));
    }

    private Object[] readInitializedParameters(String json) throws JsonProcessingException, ClassNotFoundException {
        List<Object> parameterList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        JsonNode node = objectMapper.readTree(json);
        JsonNode pa = node.get("parameters");
        for (JsonNode n : pa) {
            Object initialValue = objectMapper.readValue(n.get("value").toString(), Class.forName(n.get("type").asText()));
            parameterList.add(initialValue);
        }
        return parameterList.toArray();

    }

    private Class<?>[] readClasses(String json) {
        JSONObject o = new JSONObject(json);
        JSONArray p = o.getJSONArray("parameters");
        List<Class<?>> classList = new ArrayList<>();
        p.forEach(entry -> {
            JSONObject e = (JSONObject) entry;
            try {
                classList.add(Class.forName(e.getString("type")));
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        });
        return classList.toArray(new Class<?>[0]);
    }


    public String getName() {
        return name;
    }

    public int getWindowSize() {
        return stateWindowList.size();
    }

    public List<StateWindow> getStateWindow() {
        return stateWindowList;
    }

    public List<String> getInputDataSection() {
        return inputDataSection;
    }

    public List<String> getOutputDataSections() {
        return outputDataSections;
    }

    /**
     * Provide a label of this PET as an overview.
     * Since a switching event can be between the same PET with different parameters,
     * the label contains both information.
     * @param petDescription
     * @return
     */
    public static String getLabel(String petDescription) {
        try {
            PETDescriptor descriptor = new PETDescriptor(petDescription);
            StringBuilder stringBuilder = new StringBuilder(descriptor.getName());
            for (Object o : descriptor.parameterWithInitialValues){
                stringBuilder.append(o.toString());
            }
            return stringBuilder.toString();
        } catch (ClassNotFoundException c) {
            System.out.println("Class not found.");
            return null;
        } catch (JsonProcessingException j) {
            System.out.println("Bad JSON format.");
            return null;
        }
    }

    public Class<?>[] getConstructorClasses() {
        return constructorClasses;
    }

    public Object[] getParameterWithInitialValues() {
        return parameterWithInitialValues;
    }

    /**
     * Check if this PET needs to build a state.
     *
     * @return true if yes
     */
    public boolean isStateful() {
        for (StateWindow sw : stateWindowList) {
            if (sw.isStateful()) {
                return true;
            }
        }
        return false;
    }

    public static class StateWindow {
        public enum WindowType {
            TIME, PIECE
        }

        private final String dataSection;
        private final WindowType type;
        private final int size;

        public StateWindow(String dataSection, WindowType type, int amount) {
            this.dataSection = dataSection;
            this.type = type;
            this.size = amount;
        }

        public StateWindow(JSONObject object) {
            this.dataSection = object.getString("dataSection");
            String typeInJSON = object.getString("type").toUpperCase();
            switch (typeInJSON) {
                case "TIME" -> this.type = WindowType.TIME;
                case "PIECE" -> this.type = WindowType.PIECE;
                default -> throw new IllegalArgumentException(typeInJSON + " is not a valid type of StateWindow.");
            }
            this.size = object.getInt("size");
        }

        public WindowType getType() {
            return type;
        }

        public int getSize() {
            return size;
        }

        public String getDataSection() {
            return dataSection;
        }

        public boolean isStateful() {
            if (this.type.equals(WindowType.TIME)) return true;
            else return this.size > 1;
        }

        @Override
        public String toString() {
            if (type.equals(WindowType.TIME)) {
                return "Records in last " + size + "s";
            } else {
                return "last " + size + " records";
            }
        }
    }
}
