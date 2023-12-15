package thesis.pet.repo;

import org.apache.commons.math3.distribution.BetaDistribution;

import java.awt.geom.Point2D;
import java.util.*;
import java.util.stream.Collectors;

public class LocationAnonymizer {
	
	// private static final Variance VARIANCE = new Variance();
	
	private final List<LocationWrapper> locations; // n locations
	private final int candidateAmount; // n - 1
	private final List<Double> intervals;
	
	private final int k;
	
	private final int m;
	private final double gamma;
	
	public LocationAnonymizer(List<Point2D.Double> locations, int k, int m, double gamma) {
		this.locations = locations.stream().map(l -> new LocationWrapper(l)).collect(Collectors.toCollection(ArrayList::new));
		
		this.candidateAmount = this.locations.size() - 1;
		this.intervals = initIntervals();
		this.k = k;
		this.m = m;
		this.gamma = gamma;
	}
	
	public LocationAnonymizer(int k, int m, double gamma, LocationWrapper... locations) {
		this.locations = new ArrayList<>(Arrays.asList(locations));
		Collections.sort(this.locations);
		
		this.candidateAmount = this.locations.size() - 1;
		this.intervals = initIntervals();
		this.k = k;
		this.m = m;
		this.gamma = gamma;
	}	
	
	public List<Point2D.Double> generate(int realLocationIndex) {
		if (realLocationIndex < 0 || realLocationIndex >= locations.size())
			throw new IndexOutOfBoundsException(realLocationIndex);
		
		BetaDistribution distribution = computeBetaDistribution(realLocationIndex);
		
		int testIndex = 0;
		int testIndexMax = 0;
		
		int[] result = null;
		double maxEntropy = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < m; ++i) {
			//System.out.println(testIndex + ":"); // Test
			
			int[] elementIndices = generateDummyElementIndices(realLocationIndex, distribution);
			elementIndices[0] = realLocationIndex;
			
			System.out.println("\\hline");
			
			System.out.print(i + " & ");
			System.out.print("\\{");
			for (int x : elementIndices) {
				System.out.print(x + ",");
				// System.out.print(locations.get(x) + " ");
			}
			System.out.print("\\} ");
			
			System.out.print("& ");
			
			double entropy = computeEntropyOfCondProbs(elementIndices);
			if (entropy > maxEntropy) {
				result = elementIndices;
				maxEntropy = entropy;
				
				testIndexMax = testIndex;
			}
			

			int ro = 1000;
			System.out.print(((double) Math.round(entropy * ro)) / ro);
			System.out.print(" \\\\ ");
			System.out.println();
			
			++testIndex;
		}
		
		List<Point2D.Double> resultLocations = new ArrayList<>();
		for (int i : result) {
			LocationWrapper location = locations.get(i);
			location.incrementAmount();
			resultLocations.add(location.getPoint());
		}
		reSort();
		
		System.out.println("Result:");
		System.out.println("Index: " + testIndexMax);
		System.out.println(resultLocations);
		System.out.println("Entropy: " + maxEntropy);
		
		return resultLocations;
	}
	
	private double computeEntropyOfCondProbs(int[] elementIndices) {		
		double[] probs = new double[elementIndices.length];
		for (int i = 0; i < elementIndices.length; ++i) {
			int real = elementIndices[i];
			double prob = 1D;
			BetaDistribution d = computeBetaDistribution(real);
			for (int e : elementIndices) {
				if (real == e)
					continue;
				
				prob *= computeProbability(e, real, d);
			}
			probs[i] = prob;
		}
		
		for (double p : probs) {
			System.out.print(MathUtil.roundToPercent(p, 3) + " & ");
		}
		
		// return VARIANCE.evaluate(probs);
		return MathUtil.entropy(probs);
	}
	
	private double computeProbability(int elementIndex, int realLocationIndex, BetaDistribution distribution) {
		int intervalIndex = elementIndexToIntervalIndex(elementIndex, realLocationIndex);
		return getIntervalProbability(intervalIndex, distribution);
	}

	/**
	 * 0 <= r <= n 
	 * 
	 * @param realLocationIndex
	 * @return a BetaDistribution
	 */
	private BetaDistribution computeBetaDistribution(int realLocationIndex) {
		double mode = intervals.get(realLocationIndex);		
		return MathUtil.computeBetaDistribution(mode, gamma);
	}
	
	private int[] generateDummyElementIndices(int realLocationIndex, BetaDistribution distribution) {
		Set<Integer> intervalIndices = new HashSet<>(); // Set so we avoid duplicates		
		do {
			double value = distribution.sample();
			intervalIndices.add(getIntervalIndex(value));
			
		} while (intervalIndices.size() < k - 1);
		
		int[] elementIndices = new int[k];
		int i = 1; // index 0 reserved for real location
		for (int element : intervalIndices) {
			elementIndices[i] = intervalIndexToElementIndex(element, realLocationIndex);
			++i;
		}
		
		return elementIndices;
	}

	private int getIntervalIndex(double value) {
		int intervalIndex = (int) (value * candidateAmount);
		
		if (intervalIndex == candidateAmount) // if value == 1 (rare, but theoretically possible)
			intervalIndex = candidateAmount - 1;
		
		return intervalIndex;	
	}
	
	private double getIntervalProbability(int intervalIndex, BetaDistribution distribution) {
		double lower = intervals.get(intervalIndex);
		double upper = intervals.get(intervalIndex + 1);
		
		return distribution.probability(lower, upper);
	}
	
	private int intervalIndexToElementIndex(int intervalIndex, int realLocationIndex) {
		if (intervalIndex >= realLocationIndex)
			return intervalIndex + 1;
		else	
			return intervalIndex;	
	}
	
	private int elementIndexToIntervalIndex(int elementIndex, int realLocationIndex) {
		if (elementIndex > realLocationIndex)
			return elementIndex - 1;
		else
			return elementIndex;
		
	}
	
	private List<Double> initIntervals() {
		List<Double> intervals = new ArrayList<>();
		for (int i = 0; i < candidateAmount; ++i) {
			intervals.add(i / (double) candidateAmount);
		}
		intervals.add(1.0D);
		
		return intervals;
	}
	
	private void reSort() {
		Collections.sort(locations);
	}
	
	public static double calcAlpha(double mode, double beta) {
		return MathUtil.calcAlpha(mode, beta);
	}

	public static void main(String[] args) {
		// TEST
		final int n = 100;
		
		final int k = 3;
		final int m = 15;
		final double gamma = 10.0D;
		final int realLocationIndex = 40;
		
		LocationWrapper[] loc = new LocationWrapper[n];
		for (int i = 0; i < n; ++i) {
			loc[i] = new LocationWrapper(new Point2D.Double(i, i), i);
		}

		loc[realLocationIndex].getPoint().setLocation(68.9, 96.8);

		
		long time = System.currentTimeMillis();
		LocationAnonymizer locAno = new LocationAnonymizer(k, m, gamma, loc);

		List<Point2D.Double> result = locAno.generate(realLocationIndex);
		
		System.out.println("Result: "+result.get(2).toString());
		System.out.println("Time: " + (System.currentTimeMillis() - time));
	}
}
