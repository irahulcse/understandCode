package thesis.pet.repo;

import java.text.DecimalFormat;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.util.FastMath;

public final class MathUtil {
	
	private MathUtil() { }
	
	public static final double entropy(double... probabilities) {
		double sum = 0.0D;
		for (double p : probabilities) {
			if (p <= 0)
				continue;
			
			sum += p * FastMath.log(2, p);
		}
		
		return -sum;
	}
	
	public static final double round(double d, int decimals) {
		double r = Math.pow(10, decimals);
		return ((double) Math.round(d * r)) / r;
	}
	
	public static final String roundToPercent(double d, int decimals) {
		String i = "";
		for (int k = 0; k < decimals; ++k)
			i += "#";
		
		DecimalFormat decimalFormat = new DecimalFormat("##." + i + "\\%");
		String formatted = decimalFormat.format(d);
		return formatted;
	}
	
	public static double fit(double d, double min, double max) {
		return Math.min(Math.max(d, min), max);
	}
	
	public static BetaDistribution computeBetaDistribution(double mode, double gamma) {		
		double alpha;
		double beta;
		if (mode <= 0.5) {
			beta = gamma;
			alpha = calcAlpha(mode, beta);
		} else { // Invert beta function (swap alpha and beta)
			alpha = gamma;
			beta = calcAlpha(1 - mode, alpha);
		}
		
		// new Well19937c(3)
		return new BetaDistribution(alpha, beta);
	}
	
	public static double calcAlpha(double mode, double beta) {
		return (beta * mode - 2 * mode + 1) / (-mode + 1);
	}

}
