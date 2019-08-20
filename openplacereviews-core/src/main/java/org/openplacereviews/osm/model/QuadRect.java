package org.openplacereviews.osm.model;

public class QuadRect {
	public double minX;
	public double maxX;
	public double minY;
	public double maxY;

	public QuadRect(double minX, double minY, double maxX, double maxY) {
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}

	public QuadRect(QuadRect a) {
		this(a.minX, a.minY, a.maxX, a.maxY);
	}

	public QuadRect() {
	}

	public double width() {
		return maxX - minX;
	}

	public double height() {
		return maxY - minY;
	}

	public boolean contains(double minX, double minY, double maxX, double maxY) {
		return this.minX < this.maxX && this.minY < this.maxY && this.minX <= minX && this.minY <= minY && this.maxX >= maxX
				&& this.maxY >= maxY;
	}

	public boolean contains(QuadRect box) {
		return contains(box.minX, box.minY, box.maxX, box.maxY);
	}

	public static boolean intersects(QuadRect a, QuadRect b) {
		return a.minX < b.maxX && b.minX < a.maxX && a.minY < b.maxY && b.minY < a.maxY;
	}

	public static boolean trivialOverlap(QuadRect a, QuadRect b) {
		return !((a.maxX < b.minX) || (a.minX > b.maxX) || (a.minY < b.maxY) || (a.maxY > b.minY));
	}

	public double centerX() {
		return (minX + maxX) / 2;
	}

	public double centerY() {
		return (minY + maxY) / 2;
	}

	public void offset(double dx, double dy) {
		minX += dx;
		minY += dy;
		maxX += dx;
		maxY += dy;

	}

	public void inset(double dx, double  dy) {
		minX += dx;
		minY += dy;
		maxX -= dx;
		maxY -= dy;
	}
	
	@Override
	public String toString() {
		return "[" + (float) minX + "," + (float) minY + " - " + (float) maxX + "," + (float) maxY + "]";
	}

}