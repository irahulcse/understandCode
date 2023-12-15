package thesis.pet.repo;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import thesis.pet.repo.MathUtil;

import java.time.Duration;
import java.time.Instant;

public class SpeedMapping extends RichMapFunction<Double, Double> {

    private double min;
    private double max;
    private double gamma;
    private Duration relax;
    private double defaultDeviation;

    private double lastValue;
    private Instant lastTime;

    public SpeedMapping(double min, double max, double gamma, Duration relax, double defaultDeviation) {
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
        this.lastValue = lastValue;
        this.lastTime = lastTime;
    }

    @Override
    public Double map(Double value) throws Exception {
        return process(value, Instant.now());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //UserCodeClassLoader userCodeClassLoader = (UserCodeClassLoader) getRuntimeContext().getUserCodeClassLoader();


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
}
