package thesis.pet.repo;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.LocationData;
import thesis.pet.PETFragment;

import java.awt.geom.Point2D;
import java.util.*;

public class AdaptiveGeoPET implements PETFragment {

    private final List<LocationData> obfuscated;
    private Queue<LocationData> dataForState;
    private List<Double> latitudeWindow;
    private List<Double> longitudeWindow;
    private List<Long> timestampWindow;
    private final int size;
    private final SimpleRegression srLatitude, srLongitude;
    private int startingPos = 0;
    private final double threshold1, threshold2, epsilon, alpha, beta;

    public AdaptiveGeoPET(Integer size, Double threshold1, Double threshold2, Double epsilon, Double alpha, Double beta) {
        this.size = size;
        this.threshold1 = threshold1;
        this.threshold2 = threshold2;
        this.epsilon = epsilon;
        this.alpha = alpha;
        this.beta = beta;
        obfuscated = new ArrayList<>(size);
        latitudeWindow = new ArrayList<>(size);
        longitudeWindow = new ArrayList<>(size);
        timestampWindow = new ArrayList<>(size);
        dataForState = new ArrayDeque<>();
        srLatitude = new SimpleRegression();
        srLongitude = new SimpleRegression();
        //System.out.println("AdaptiveGEO: instantiated with size "+size);
    }

    @Override
    public VehicleContext execute(VehicleContext in) {
        LocationData locationData = in.getLocationData();
        LocationData processedData = process(locationData);
        in.update(processedData);
        return in;
    }

    public LocationData process(LocationData locationData) {
        if (longitudeWindow.size() < size - 1) throw new RuntimeException("The PET is not warmed up yet.");
        if (longitudeWindow.size() >= size) {
            latitudeWindow = new ArrayList<>(latitudeWindow.subList(1, size));
            longitudeWindow = new ArrayList<>(longitudeWindow.subList(1, size));
            timestampWindow = new ArrayList<>(timestampWindow.subList(1, size));
        }
        addRecord(locationData);
        LocationData obfuscation;
        // perform simple linear regression
        double latitude = calculateOrdinate(srLatitude, timestampWindow, latitudeWindow, locationData.getGenerateTime());
        double longitude = calculateOrdinate(srLongitude, timestampWindow, longitudeWindow, locationData.getGenerateTime());
        Point2D.Double predicted = new Point2D.Double(latitude, longitude);
        Point2D original = locationData.getData();
        double distance = Math.abs(original.distance(predicted));
        System.out.println("Distance = " + distance);
        double adjustedEpsilon;
        if (distance < threshold1) {
            adjustedEpsilon = alpha * epsilon;
        } else if (distance < threshold2) {
            adjustedEpsilon = epsilon;
        } else {
            adjustedEpsilon = beta * epsilon;
        }
        System.out.println("Adjusted epsilon = " + adjustedEpsilon);
        LaplaceDistribution lp = new LaplaceDistribution(adjustedEpsilon, distance);
        double sampledLatitude = lp.sample();
        double degree = Math.random() * 360;
        Point2D.Double newPoint = new Point2D.Double(locationData.getData().getX() + sampledLatitude * Math.cos(Math.toRadians(degree)),
                locationData.getData().getY() + sampledLatitude * Math.sin(Math.toRadians(degree)));


        obfuscation = new LocationData(newPoint);
//        output.setProcessTime(System.currentTimeMillis());
        obfuscated.add(startingPos, obfuscation);
        locationData.setProcessedData(newPoint);
        locationData.setProcessTime(System.currentTimeMillis());
        latitudeWindow.add(startingPos, obfuscation.getData().getX());
        longitudeWindow.add(startingPos, obfuscation.getData().getY());

        incrementStartingPosition();
        backUpOriginalRecord(locationData);

        return locationData;
    }


    private double calculateOrdinate(SimpleRegression regression, List<Long> timestamps, List<Double> values, long timestamp) {
        for (int i = 0; i < size; i++) {
            regression.addData(timestamps.get(i), values.get(i));
        }
        return regression.predict(timestamp);
    }

    private void incrementStartingPosition() {
        if (startingPos == size - 1) {
            startingPos = 0;
        } else {
            startingPos++;
        }
    }


    @Override
    public void buildState(VehicleContext record) {
        LocationData ld = record.getLocationData();
        buildWithRawData(ld);

    }

    @Override
    public void buildState(Map.Entry<String, List<? extends Data<?>>> records) {
        List<LocationData> warmUpData = new ArrayList<>();
        if (records.getKey().equals("location")) {
            for (Data<?> data : records.getValue()) {
                warmUpData.add((LocationData) data);
            }
        }
        buildWithRawData(warmUpData);
        PETFragment.super.buildState(records);
    }

    public void buildWithRawData(LocationData record) {

        if (latitudeWindow.size() <= size) {
            addRecord(record);
        } else {
            latitudeWindow = new ArrayList<>(latitudeWindow.subList(1, size - 1));
            longitudeWindow = new ArrayList<>(longitudeWindow.subList(1, size - 1));
            timestampWindow = new ArrayList<>(timestampWindow.subList(1, size - 1));
            addRecord(record);
        }
        backUpOriginalRecord(record);
    }

    public void buildWithRawData(List<LocationData> records) {
        if (records.size() > size) {
            PriorityQueue<LocationData> sorted = new PriorityQueue<>(new RecordsComparator<>());
            sorted.addAll(records);
            for (int i = 0; i < size; i++) {
                buildWithRawData(sorted.poll());
            }
        } else {
            for (LocationData ld : records) {
                buildWithRawData(ld);
            }
        }
    }

    private void addRecord(LocationData record) {
        latitudeWindow.add(record.getData().getX());
        longitudeWindow.add(record.getData().getY());
        timestampWindow.add(record.getGenerateTime());
    }

    private void backUpOriginalRecord(LocationData record) {
        if (dataForState.size() <= size) {
            dataForState.add(record);
        } else {
            dataForState.poll();
            dataForState.add(record);
        }
    }

    @Override
    public boolean isReady() {
        return PETFragment.super.isReady();
    }

    private static class RecordsComparator<T extends Data<?>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return (int) (o1.getGenerateTime() - o2.getGenerateTime());
        }
    }


/*
    public static void main(String[] args) {
        List<Point2D.Double> pointList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Point2D.Double point = new Point2D.Double(i, i + 1D);
            pointList.add(point);
        }
        LocationData ld = new LocationData(new Point2D.Double(6D, 9D));
        double epsilon = 0.1;
        int size = 5;
        AdaptiveGeoPET adaptiveGeoPET = new AdaptiveGeoPET(size, 0.96 / epsilon, 2.7 / epsilon, 0.1, 0.1, 5.0);
        for (Point2D p : pointList) {
            LocationData l = new LocationData(p);
            System.out.println("Input: " + l);
            System.out.println("Output: " + adaptiveGeoPET.execute(l));
        }
    }
*/

}
