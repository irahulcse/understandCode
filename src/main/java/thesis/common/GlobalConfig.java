package thesis.common;

import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import java.awt.geom.Point2D;
import java.util.Map;

/**
 * Stores the global constant variable, such as the name of the topic, of MongoDB collection, or the paths to python runtime, to pet-description, to policy description, etc...
 * If the types of sensor reading should be extended, for example, adding lidar data, or temperature data,
 * the mapping relation {@link #recordRawClassMap} and {@link #sectionRecordClassMap} should also be extended
 */
public class GlobalConfig {

    // Address
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String dbURL = "mongodb://localhost:27017";
    // Python
    public static final String imagePETRuntime = "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3";
    public static final String pythonRuntime = "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3";
    public static final String analyzerPath = "/Users/rahulchandra/Downloads/FAPRA/ma-pet-live-adaptation/image_resources/Main.py";
    // Topics
    public static final String INPUT_TOPIC = "test-topic";
    public static final String SCALAR_TOPIC= "scalar";
    public static final String LOCATION_TOPIC = "location";
    public static final String IMAGE_TOPIC = "image";
    public static final String POLICY_TOPIC = "policy";
    public static final String SWITCHING_TOPIC = "switching-ready";
    // MongoDB Collection names
    public static final String POLICY_COLLECTION = POLICY_TOPIC;
    public static final String SPEED_COLLECTION = "speed";
    public static final String LOCATION_COLLECTION = LOCATION_TOPIC;
    public static final String IMAGE_COLLECTION = IMAGE_TOPIC;
    // Input paths
    ///Users/rahulchandra/Downloads/FAPRA/ma-pet-live-adaptation/textInputSources
    public static final String textInputSource = "/Users/rahulchandra/Downloads/FAPRA/ma-pet-live-adaptation/textInputSources/";
    public static final String csvOutputPath = "/Users/rahulchandra/Downloads/FAPRA/ma-pet-live-adaptation/result/";
    public static final String imageSourcePath = "/Users/rahulchandra/Downloads/FAPRA/image_resources/";
    // Mapping relations
    public static final Map<String, Class<? extends Data<?>>> sectionRecordClassMap = Map.of("speed",ScalarData.class, "location", LocationData.class, "image", ImageData.class);
    public static final Map<Class<? extends Data<?>>, Class<?>> recordRawClassMap = Map.of(ScalarData.class, Double.class, LocationData.class, Point2D.class, ImageData.class, byte[].class);

}
