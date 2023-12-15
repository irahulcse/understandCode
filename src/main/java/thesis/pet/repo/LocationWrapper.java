package thesis.pet.repo;

import java.awt.geom.Point2D;

public class LocationWrapper implements Comparable<LocationWrapper> {
	
	private final Point2D.Double point;
	private long amount;
	
	public LocationWrapper(Point2D.Double point, long amount) {
		this.point = point;
		this.amount = amount;
	}
	
	public LocationWrapper(Point2D.Double point) {
		this(point, 0L);
	}
	
	public void incrementAmount() {
		++amount;
	}

	/**
	 * @return the amount
	 */
	public long getAmount() {
		return amount;
	}

	/**
	 * @param amount the amount to set
	 */
	public void setAmount(int amount) {
		this.amount = amount;
	}

	/**
	 * @return the point
	 */
	public Point2D.Double getPoint() {
		return point;
	}

	@Override
	public int compareTo(LocationWrapper other) {
		if (this.amount == other.amount)
			return 0;
		else if (this.amount < other.amount)
			return -1;
		else
			return 1;
	}
	
	@Override
	public String toString() {
		return "[" + point + " | " + amount + "]";
	}
}
