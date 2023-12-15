package thesis.pet.repo;

import org.apache.commons.math3.distribution.BetaDistribution;
import thesis.context.VehicleContext;
import thesis.context.data.ScalarData;
import thesis.pet.PETFragment;

import java.time.Duration;
import java.time.Instant;

/**
 * Adapted from BA: Dominik Held / Privacy-Software-Defined-Car
 */
public class SpeedAnonymizer implements PETFragment {

    private double min;
    private double max;
    private double gamma;
    private Duration relax;
    private double defaultDeviation;

    private double lastValue;
    private Instant lastTime;

    public SpeedAnonymizer(Double min, Double max, Double gamma, Duration relax, Double defaultDeviation) {
        if (min > max)
            throw new IllegalArgumentException("min cannot be bigger than max");

        if (relax == null)
            throw new IllegalArgumentException("relax cannot be null");

        if (gamma <= 1)
            throw new IllegalArgumentException("gamma must be bigger than 1");

        if (relax.isNegative() || relax.isZero())
            throw new IllegalArgumentException("relax duration must be positive");

        if (defaultDeviation < 0)
            throw new IllegalArgumentException("defaultDeviation cannot be negative");

        this.min = min;
        this.max = max;
        this.gamma = gamma;
        this.relax = relax;
        this.defaultDeviation = defaultDeviation;

        this.lastValue = -1;
        this.lastTime = null;
    }

    public double process(double speed) {
        return process(speed, Instant.now());
    }

    public double process(double speed, Instant time) {
        Duration sinceLast;
        if (lastTime == null)
            sinceLast = Duration.ofMillis(0);
        else {
            sinceLast = Duration.between(lastTime, time);
            if (sinceLast.isNegative())
                throw new IllegalArgumentException("time argument is before the last time argument.");
        }

        if (lastValue < min || lastValue > max)
            lastValue = MathUtil.fit(speed, min, max);

        double factor = sinceLast.toNanos() / (double) relax.toNanos();

        double deviation = factor * defaultDeviation;
        double lower = Math.max(min, lastValue - deviation);
        double upper = Math.min(max, lastValue + deviation);

        double result;
        if (lower != upper) {
            double pivot = MathUtil.fit(speed, lower, upper);

            double mode = (pivot - lower) / (upper - lower);

            BetaDistribution distr = MathUtil.computeBetaDistribution(mode, gamma);
            result = lower + (upper - lower) * distr.sample();
        } else {
            result = upper;
        }

        lastValue = result;
        lastTime = time;

        return result;
    }


    /**
     * @return the min
     */
    public double getMin() {
        return min;
    }

    /**
     * @param min the min to set
     */
    public void setMin(double min) {
        this.min = min;
    }

    /**
     * @return the max
     */
    public double getMax() {
        return max;
    }

    /**
     * @param max the max to set
     */
    public void setMax(double max) {
        this.max = max;
    }

    /**
     * @return the relax
     */
    public Duration getRelax() {
        return relax;
    }

    /**
     * @param relax the relax to set
     */
    public void setRelax(Duration relax) {
        this.relax = relax;
    }

    /**
     * @return the gamma
     */
    public double getGamma() {
        return gamma;
    }

    /**
     * @param gamma the gamma to set
     */
    public void setGamma(double gamma) {
        this.gamma = gamma;
    }

    /**
     * @return the defaultDeviation
     */
    public double getDefaultDeviation() {
        return defaultDeviation;
    }

    /**
     * @param defaultDeviation the defaultDeviation to set
     */
    public void setDefaultDeviation(double defaultDeviation) {
        this.defaultDeviation = defaultDeviation;
    }

    /**
     * @return the lastValue
     */
    public double getLastValue() {
        return lastValue;
    }

    /**
     * @param lastValue the lastValue to set
     */
    public void setLastValue(double lastValue) {
        this.lastValue = lastValue;
    }

    /**
     * @return the lastTime
     */
    public Instant getLastTime() {
        return lastTime;
    }

    /**
     * @param lastTime the lastTime to set
     */
    public void setLastTime(Instant lastTime) {
        this.lastTime = lastTime;
    }


    public ScalarData process(ScalarData scalarData) {
        Double d = scalarData.getData();
        Double dp = process(d);
        scalarData.setProcessedData(dp);
        scalarData.setProcessTime(System.currentTimeMillis());
        return scalarData;
    }

    @Override
    public VehicleContext execute(VehicleContext in) {
        ScalarData sd = in.getScalarData();
        ScalarData result = process(sd);
        in.update(result);
        return in;
    }
}
