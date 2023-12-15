package thesis.util;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import thesis.common.GlobalConfig;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;
import thesis.pet.PETDescriptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static thesis.common.GlobalConfig.*;

/**
 * The class that takes the responsibility of initializing the database and
 * hide the details of data access from the modularized PETs
 */
public class DBWrapper {

    private final MongoClient client;
    public static MongoDatabase db;

    private static final Map<String, MongoCollection<Document>> dataSectionCollectionMap = new HashMap<>();

    public DBWrapper() {
        try {
            client = MongoClients.create(GlobalConfig.dbURL);
            db = client.getDatabase("framework");
            try {
                db.createCollection(SPEED_COLLECTION);
                db.createCollection(POLICY_COLLECTION);
            } catch (Exception ignored) {
            }
            clearCollection(SPEED_COLLECTION);
            initializeMap();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MongoDatabase getDb() {
        return db;
    }

    /**
     * A method that retrieves necessary data from the database to warm up the next PET,
     * if it needs to build a state before start processing the records.
     * It checks the data section specified in {@link PETDescriptor.StateWindow} and fetches the corresponding data records from DB.
     * The class of the data record should be given explicitly.
     *
     * @param stateWindow The description of the {@link thesis.pet.PETDescriptor.StateWindow}
     * @param recordClass The type of the data record, for example {@link ScalarData}
     * @param <T>         The class of the data record
     * @return {@link java.util.Map.Entry} with the name of the data section as key and a list of data records as value
     */
    public static <T extends Data<?>> Map.Entry<String, List<T>> prepareWarmUp(PETDescriptor.StateWindow stateWindow, Class<T> recordClass, VehicleContext vc) {
        String dataSection = stateWindow.getDataSection();
        PETDescriptor.StateWindow.WindowType windowType = stateWindow.getType();
        T data = vc.extractRecord(recordClass);
        int size = stateWindow.getSize();
        List<T> dataList = new ArrayList<>();
        MongoCollection<Document> collection = dataSectionCollectionMap.get(dataSection);
        if (windowType.equals(PETDescriptor.StateWindow.WindowType.TIME)) {
            Long startTimestamp = data.getGenerateTime() - size * 1000L; // Find the starting point. Size represents "seconds".
            Bson filter = Filters.gt("generationTime", startTimestamp);
            FindIterable<Document> results = collection.find(filter);
            for (Document doc : results) {
                Object dataRecord = fromDocToRecord(doc, recordClass);
                dataList.add(recordClass.cast(dataRecord));
            }
        } else {
            boolean enoughRecords = (collection.countDocuments() - size) >= 0;
            FindIterable<Document> results;
            if (enoughRecords) {
                long baseLineTimestamp = data.getGenerateTime();
                Bson filter = Filters.lt("generationTime", baseLineTimestamp);
                Bson order = Sorts.descending("generationTime");
                results = collection.find(filter).sort(order).limit(size - 1);
            } else {
                results = collection.find();
            }
            for (Document doc : results) {
                Object dataRecord = fromDocToRecord(doc, recordClass);
                dataList.add(recordClass.cast(dataRecord));
            }
        }
        return Map.entry(dataSection, dataList);

    }

    /**
     * A method that transforms the query result from the database to the corresponding data record.
     * The class of the raw data and the class of the target data record should be explicitly given.
     *
     * @param doc
     * @param rawDataClass the class of the raw sensor reading, for example {@link Double}
     * @param recordClass  the class of the data record
     * @param <RC>         The class of the data record
     * @param <RD>         The class of the raw data
     * @return
     */
    private static <RC extends Data<?>, RD> RC generateRecord(Document doc, Class<RD> rawDataClass, Class<RC> recordClass) {
        String dataSection = doc.getString("dataSection");
        RD raw = doc.get("data", rawDataClass);
        long timestamp = doc.getLong("generationTime");

        try {
            Constructor<RC> constructor = recordClass.getConstructor(String.class, rawDataClass, Long.class);
            return constructor.newInstance(dataSection, raw, timestamp);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <RC extends Data<?>> Object fromDocToRecord(Document doc, Class<RC> recordClass) {
        Class<?> rawClass = GlobalConfig.recordRawClassMap.get(recordClass);
        return recordClass.cast(generateRecord(doc, rawClass, recordClass));
    }

    private void initializeMap() {
        dataSectionCollectionMap.put("speed", db.getCollection(SPEED_COLLECTION));
        dataSectionCollectionMap.put("location", db.getCollection(LOCATION_COLLECTION));
        dataSectionCollectionMap.put("image", db.getCollection(IMAGE_COLLECTION));
    }

    private void clearCollection(String name) {
        MongoCollection<Document> collection = db.getCollection(name);
        collection.deleteMany(new Document());
    }

    public static void writeToDB(Data<?> data){
        MongoCollection<Document> collection;
        if (data instanceof ScalarData){
            collection = db.getCollection(data.dataSection);
        }else if (data instanceof LocationData){
            collection = db.getCollection("location");
        }else if (data instanceof ImageData){
            collection = db.getCollection("image");
        }else {
            throw new RuntimeException("Unhandled class in DB: " + data.getClass().getName());
        }
        Document document = data.toMongoDBDocument();
        collection.insertOne(document);
    }

}
