package org.openplacereviews.db.model;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.openplacereviews.db.model.Relation.RelationMember;

public class OsmMapUtils {
	public static final double MIN_LATITUDE = -85.0511;
	public static final double MAX_LATITUDE = 85.0511;
	public static final double LATITUDE_TURN = 180.0;
	public static final double MIN_LONGITUDE = -180.0;
	public static final double MAX_LONGITUDE = 180.0;
	public static final double LONGITUDE_TURN = 360.0;


	public static double getPowZoom(double zoom) {
		if (zoom >= 0 && zoom - Math.floor(zoom) < 0.001f) {
			return 1 << ((int) zoom);
		} else {
			return Math.pow(2, zoom);
		}
	}
	
	private static double toRadians(double angdeg) {
//		return Math.toRadians(angdeg);
		return angdeg / 180.0 * Math.PI;
	}
	

	/**
	 * Theses methods operate with degrees (evaluating tiles & vice versa)
	 * degree longitude measurements (-180, 180) [27.56 Minsk]
	 * // degree latitude measurements (90, -90) [53.9]
	 */

	public static double getTileNumberX(float zoom, double longitude) {
		longitude = checkLongitude(longitude);
		final double powZoom = getPowZoom(zoom);
		double dz = (longitude + 180d)/360d * powZoom;
		if (dz >= powZoom) {
			return powZoom - 0.01;
		}
		return dz;
	}

	public static double getTileNumberY(float zoom, double latitude) {
		latitude = checkLatitude(latitude);
		double eval = Math.log(Math.tan(toRadians(latitude)) + 1/Math.cos(toRadians(latitude)));
		if (Double.isInfinite(eval) || Double.isNaN(eval)) {
			latitude = latitude < 0 ? -89.9 : 89.9;
			eval = Math.log(Math.tan(toRadians(latitude)) + 1/Math.cos(toRadians(latitude)));
		}
		return (1 - eval / Math.PI) / 2 * getPowZoom(zoom);
	}


	public static double checkLongitude(double longitude) {
		if (longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE) {
			return longitude;
		}
		while (longitude <= MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
			if (longitude < 0) {
				longitude += LONGITUDE_TURN;
			} else {
				longitude -= LONGITUDE_TURN;
			}
		}
		return longitude;
	}

	public static double checkLatitude(double latitude) {
		if (latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE) {
			return latitude;
		}
		while (latitude < -90 || latitude > 90) {
			if (latitude < 0) {
				latitude += LATITUDE_TURN;
			} else {
				latitude -= LATITUDE_TURN;
			}
		}
		if (latitude < MIN_LATITUDE) {
			return MIN_LATITUDE;
		} else if (latitude > MAX_LATITUDE) {
			return MAX_LATITUDE;
		}
		return latitude;
	}

	
	/**
	 * Gets distance in meters
	 */
	public static double getDistance(double lat1, double lon1, double lat2, double lon2) {
		double R = 6372.8; // for haversine use R = 6372.8 km instead of 6371 km
		double dLat = toRadians(lat2 - lat1);
		double dLon = toRadians(lon2 - lon1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
		        Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) *
		        Math.sin(dLon / 2) * Math.sin(dLon / 2);
		//double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//return R * c * 1000;
		// simplyfy haversine:
		return (2 * R * 1000 * Math.asin(Math.sqrt(a)));
	}
	
	public static double getDistance(LatLon l, double latitude, double longitude) {
		return getDistance(l.getLatitude(), l.getLongitude(), latitude, longitude);
	}
	
	public static double getDistance(Node e1, Node e2) {
		return getDistance(e1.getLatitude(), e1.getLongitude(), e2.getLatitude(), e2.getLongitude());
	}

	public static double getDistance(Node e1, double latitude, double longitude) {
		return getDistance(e1.getLatitude(), e1.getLongitude(), latitude, longitude);
	}

	public static double getDistance(Node e1, LatLon point) {
		return getDistance(e1.getLatitude(), e1.getLongitude(), point.getLatitude(), point.getLongitude());
	}
	

	private static double scalarMultiplication(double xA, double yA, double xB, double yB, double xC, double yC) {
		// Scalar multiplication between (AB, AC)
		return (xB - xA) * (xC - xA) + (yB - yA) * (yC - yA);
	}

	public static double getOrthogonalDistance(double lat, double lon, double fromLat, double fromLon, double toLat, double toLon) {
		return getDistance(getProjection(lat, lon, fromLat, fromLon, toLat, toLon), lat, lon);
	}

	public static LatLon getProjection(double lat, double lon, double fromLat, double fromLon, double toLat, double toLon) {
		// not very accurate computation on sphere but for distances < 1000m it is ok
		double mDist = (fromLat - toLat) * (fromLat - toLat) + (fromLon - toLon) * (fromLon - toLon);
		double projection = scalarMultiplication(fromLat, fromLon, toLat, toLon, lat, lon);
		double prlat;
		double prlon;
		if (projection < 0) {
			prlat = fromLat;
			prlon = fromLon;
		} else if (projection >= mDist) {
			prlat = toLat;
			prlon = toLon;
		} else {
			prlat = fromLat + (toLat - fromLat) * (projection / mDist);
			prlon = fromLon + (toLon - fromLon) * (projection / mDist);
		}
		return new LatLon(prlat, prlon);
	}

	public static double getProjectionCoeff(double lat, double lon, double fromLat, double fromLon, double toLat, double toLon) {
		// not very accurate computation on sphere but for distances < 1000m it is ok
		double mDist = (fromLat - toLat) * (fromLat - toLat) + (fromLon - toLon) * (fromLon - toLon);
		double projection = scalarMultiplication(fromLat, fromLon, toLat, toLon, lat, lon);
//		double prlat;
//		double prlon;
		if (projection < 0) {
			return 0;
		} else if (projection >= mDist) {
			return 1;
		} else {
			return (projection / mDist);
		}
	}


	public static LatLon getCenter(Entity e) {
		if (e instanceof Node) {
			return ((Node) e).getLatLon();
		} else if (e instanceof Way) {
			return getWeightCenterForWay(((Way) e));
		} else if (e instanceof Relation) {
			List<LatLon> list = new ArrayList<LatLon>();
			for (RelationMember fe : ((Relation) e).getMembers()) {
				LatLon c = null;
				// skip relations to avoid circular dependencies
				if (!(fe.getEntity() instanceof Relation) && fe.getEntity() != null) {
					c = getCenter(fe.getEntity());
				}
				if (c != null) {
					list.add(c);
				}
			}
			return getWeightCenter(list);
		}
		return null;
	}

	public static LatLon getWeightCenter(Collection<LatLon> nodes) {
		if (nodes.isEmpty()) {
			return null;
		}
		double longitude = 0;
		double latitude = 0;
		for (LatLon n : nodes) {
			longitude += n.getLongitude();
			latitude += n.getLatitude();
		}
		return new LatLon(latitude / nodes.size(), longitude / nodes.size());
	}

	public static LatLon getWeightCenterForNodes(Collection<Node> nodes ) {
		if (nodes.isEmpty()) {
			return null;
		}
		double longitude = 0;
		double latitude = 0;
		int count = 0;
		for (Node n : nodes) {
			if (n != null) {
				count++;
				longitude += n.getLongitude();
				latitude += n.getLatitude();
			}
		}
		if (count == 0) {
			return null;
		}
		return new LatLon(latitude / count, longitude / count);
	}
	
	public static LatLon getWeightCenterForWay(Way w) {
		Collection<Node> nodes = w.getNodes();
		if (nodes.isEmpty()) {
			return null;
		}
		boolean area = w.getFirstNodeId() == w.getLastNodeId();
		LatLon ll = area ? getMathWeightCenterForNodes(nodes) : getWeightCenterForNodes(nodes);
		if(ll == null) {
			return null;
		}
		double flat = ll.getLatitude();
		double flon = ll.getLongitude();
		if(!area || !containsPoint(nodes, ll.getLatitude(), ll.getLongitude())) {
			double minDistance = Double.MAX_VALUE;
			for (Node n : nodes) {
				if (n != null) {
					double d = getDistance(n.getLatitude(), n.getLongitude(), ll.getLatitude(), ll.getLongitude());
					if(d < minDistance) {
						flat = n.getLatitude();
						flon = n.getLongitude();
						minDistance = d;
					}
				}
			}	
		}
		
		return new LatLon(flat, flon);
	}
	

	public static LatLon getMathWeightCenterForNodes(Collection<Node> nodes) {
		if (nodes.isEmpty()) {
			return null;
		}
		double longitude = 0;
		double latitude = 0;
		double sumDist = 0;
		Node prev = null;
		for (Node n : nodes) {
			if (n != null) {
				if (prev == null) {
					prev = n;
				} else {
					double dist = getDistance(prev, n);
					sumDist += dist;
					longitude += (prev.getLongitude() + n.getLongitude()) * dist / 2;
					latitude += (n.getLatitude() + n.getLatitude()) * dist / 2;
					prev = n;
				}
			}
		}
		if (sumDist == 0) {
			if (prev == null) {
				return null;
			}
			return prev.getLatLon();
		}
		return new LatLon(latitude / sumDist, longitude / sumDist);
	}

	public static void sortListOfEntities(List<? extends Entity> list, final double lat, final double lon) {
		Collections.sort(list, new Comparator<Entity>() {
			@Override
			public int compare(Entity o1, Entity o2) {
				return Double.compare(getDistance(o1.getLatLon(), lat, lon), getDistance(o2.getLatLon(), lat, lon));
			}
		});
	}

	public static void addIdsToList(Collection<? extends Entity> source, List<Long> ids) {
		for (Entity e : source) {
			ids.add(e.getId());
		}
	}

    public static boolean ccw(Node A, Node B, Node C) {
        return (C.getLatitude()-A.getLatitude()) * (B.getLongitude()-A.getLongitude()) > (B.getLatitude()-A.getLatitude()) *
                (C.getLongitude()-A.getLongitude());
    }

    // Return true if line segments AB and CD intersect
    public static boolean intersect2Segments(Node A, Node B, Node C, Node D) {
        return ccw(A, C, D) != ccw(B, C, D) && ccw(A, B, C) != ccw(A, B, D);
    }

	public static boolean[] simplifyDouglasPeucker(List<Node> n, int zoom, int epsilon, List<Node> result, boolean avoidNooses) {
		if (zoom > 31) {
			zoom = 31;
		}
		boolean[] kept = new boolean[n.size()];
		int first = 0;
		int nsize = n.size();
		while (first < nsize) {
			if (n.get(first) != null) {
				break;
			}
			first++;
		}
		int last = nsize - 1;
		while (last >= 0) {
			if (n.get(last) != null) {
				break;
			}
			last--;
		}
		if (last - first < 1) {
			return kept;
		}
		// check for possible cycle
		boolean checkCycle = true;
		boolean cycle = false;
		while (checkCycle && last > first) {
			checkCycle = false;

			double x1 = getTileNumberX(zoom, n.get(first).getLongitude());
			double y1 = getTileNumberY(zoom, n.get(first).getLatitude());
			double x2 = getTileNumberX(zoom, n.get(last).getLongitude());
			double y2 = getTileNumberY(zoom, n.get(last).getLatitude());
			if (Math.abs(x1 - x2) + Math.abs(y1 - y2) < 0.001) {
				last--;
				cycle = true;
				checkCycle = true;
			}
		}
		if (last - first < 1) {
			return kept;
		}
		simplifyDouglasPeucker(n, zoom, epsilon, kept, first, last, avoidNooses);
		result.add(n.get(first));
		for (int i = 0; i < kept.length; i++) {
			if(kept[i]) {
				result.add(n.get(i));
			}
		}
		if (cycle) {
			result.add(n.get(first));
		}
		kept[first] = true;
		
		return kept;
	}

	private static void simplifyDouglasPeucker(List<Node> n, int zoom, int epsilon, boolean[] kept,
                                               int start, int end, boolean avoidNooses) {
		double dmax = -1;
		int index = -1;
		for (int i = start + 1; i <= end - 1; i++) {
			if (n.get(i) == null) {
				continue;
			}
			double d = orthogonalDistance(zoom, n.get(start), n.get(end), n.get(i));// calculate distance from line
			if (d > dmax) {
				dmax = d;
				index = i;
			}
		}
        boolean nooseFound = false;
        if(avoidNooses && index >= 0) {
            Node st = n.get(start);
            Node e = n.get(end);
            for(int i = 0; i < n.size() - 1; i++) {
                if(i == start - 1) {
                    i = end;
                    continue;
                }
                Node np = n.get(i);
                Node np2 = n.get(i + 1);
                if(np == null || np2 == null) {
                    continue;
                }
                if (OsmMapUtils.intersect2Segments(st, e, np, np2)) {
                    nooseFound = true;
                    break;
                }
            }
        }
		if (dmax >= epsilon || nooseFound ) {
			simplifyDouglasPeucker(n, zoom, epsilon, kept, start, index, avoidNooses);
			simplifyDouglasPeucker(n, zoom, epsilon, kept, index, end, avoidNooses);
		} else {
			kept[end] = true;
		}
	}

	private static double orthogonalDistance(int zoom, Node nodeLineStart, Node nodeLineEnd, Node node) {
		LatLon p = getProjection(node.getLatitude(), node.getLongitude(), nodeLineStart.getLatitude(),
				nodeLineStart.getLongitude(), nodeLineEnd.getLatitude(), nodeLineEnd.getLongitude());

		double x1 = getTileNumberX(zoom, p.getLongitude());
		double y1 = getTileNumberY(zoom, p.getLatitude());
		double x2 = getTileNumberX(zoom, node.getLongitude());
		double y2 = getTileNumberY(zoom, node.getLatitude());
		double C = x2 - x1;
		double D = y2 - y1;
		return Math.sqrt(C * C + D * D);
	}

	public static boolean isClockwiseWay(Way w) {
		return isClockwiseWay(Collections.singletonList(w));
	}

	public static boolean isClockwiseWay(List<Way> ways) {
		if (ways.isEmpty()) {
			return true;
		}
		LatLon latLon = ways.get(0).getLatLon();
		double lat = latLon.getLatitude();
		double lon = 180;
		double firstLon = -360;
		boolean firstDirectionUp = false;
		double previousLon = -360;

		double clockwiseSum = 0;

		Node prev = null;
		boolean firstWay = true;
		for (Way w : ways) {
			List<Node> ns = w.getNodes();
			int startInd = 0;
			int nssize = ns.size();
			if (firstWay && nssize > 0) {
				prev = ns.get(0);
				startInd = 1;
				firstWay = false;
			}
			for (int i = startInd; i < nssize; i++) {
				Node next = ns.get(i);
				double rlon = ray_intersect_lon(prev, next, lat, lon);
				if (rlon != -360d) {
					boolean skipSameSide = (prev.getLatitude() <= lat) == (next.getLatitude() <= lat);
					if (skipSameSide) {
						continue;
					}
					boolean directionUp = prev.getLatitude() <= lat;
					if (firstLon == -360) {
						firstDirectionUp = directionUp;
						firstLon = rlon;
					} else {
						boolean clockwise = (!directionUp) == (previousLon < rlon);
						if (clockwise) {
							clockwiseSum += Math.abs(previousLon - rlon);
						} else {
							clockwiseSum -= Math.abs(previousLon - rlon);
						}
					}
					previousLon = rlon;
				}
				prev = next;
			}
		}

		if (firstLon != -360) {
			boolean clockwise = (!firstDirectionUp) == (previousLon < firstLon);
			if (clockwise) {
				clockwiseSum += Math.abs(previousLon - firstLon);
			} else {
				clockwiseSum -= Math.abs(previousLon - firstLon);
			}
		}

		return clockwiseSum >= 0;
	}

	// try to intersect from left to right
	public static double ray_intersect_lon(Node node, Node node2, double latitude, double longitude) {
		// a node below
		Node a = node.getLatitude() < node2.getLatitude() ? node : node2;
		// b node above
		Node b = a == node2 ? node : node2;
		if (latitude == a.getLatitude() || latitude == b.getLatitude()) {
			latitude += 0.00000001d;
		}
		if (latitude < a.getLatitude() || latitude > b.getLatitude()) {
			return -360d;
		} else {
			if (longitude < Math.min(a.getLongitude(), b.getLongitude())) {
				return -360d;
			} else {
				if (a.getLongitude() == b.getLongitude() && longitude == a.getLongitude()) {
					// the node on the boundary !!!
					return longitude;
				}
				// that tested on all cases (left/right)
				double lon = b.getLongitude() - (b.getLatitude() - latitude) * (b.getLongitude() - a.getLongitude())
						/ (b.getLatitude() - a.getLatitude());
				if (lon <= longitude) {
					return lon;
				} else {
					return -360d;
				}
			}
		}
	}

    /**
     * Get the area in pixels
     * @param nodes
     * @return
     */
    public static double polygonAreaPixels(List<Node> nodes, int zoom) {
        double area = 0.;
        double mult = 1 / getPowZoom((double)Math.max(31 - (zoom + 8), 0));
        int j = nodes.size() - 1;
        for (int i = 0; i < nodes.size(); i++) {
            Node x = nodes.get(i);
            Node y = nodes.get(j);
            if(x != null && y != null) {
            area += (get31TileNumberX(y.getLongitude()) + (double)get31TileNumberX(x.getLongitude()))*
                    (get31TileNumberY(y.getLatitude()) - (double)get31TileNumberY(x.getLatitude()));
            }
            j = i;
        }
        return Math.abs(area) * mult * mult * .5;
    }

	/**
	 * Get the area (in m²) of a closed way, represented as a list of nodes
	 * 
	 * @param nodes
	 *            the list of nodes
	 * @return the area of it
	 */
	public static double getArea(List<Node> nodes) {
		// x = longitude
		// y = latitude
		// calculate the reference point (lower left corner of the bbox)
		// start with an arbitrary value, bigger than any lat or lon
		double refX = 500, refY = 500;
		for (Node n : nodes) {
			if (n.getLatitude() < refY)
				refY = n.getLatitude();
			if (n.getLongitude() < refX)
				refX = n.getLongitude();
		}

		List<Double> xVal = new ArrayList<Double>();
		List<Double> yVal = new ArrayList<Double>();

		for (Node n : nodes) {
			// distance from bottom line to x coordinate of node
			double xDist = getDistance(refY, refX, refY, n.getLongitude());
			// distance from left line to y coordinate of node
			double yDist = getDistance(refY, refX, n.getLatitude(), refX);

			xVal.add(xDist);
			yVal.add(yDist);
		}

		double area = 0;

		for (int i = 1; i < xVal.size(); i++) {
			area += xVal.get(i - 1) * yVal.get(i) - xVal.get(i) * yVal.get(i - 1);
		}

		return Math.abs(area) / 2;
	}


	public static boolean containsPoint(Collection<Node> polyNodes, double latitude, double longitude) {
		return countIntersections(polyNodes, latitude, longitude) % 2 == 1;
	}
	
	/**
	 * count the intersections when going from lat, lon to outside the ring
	 * @param polyNodes2 
	 */
	private static int countIntersections(Collection<Node> polyNodes, double latitude, double longitude) {
		int intersections = 0;
		if (polyNodes.size() == 0)
			return 0;
		Node prev = null;
		Node first = null;
		Node last = null;
		for(Node n  : polyNodes) {
			if(prev == null) {
				prev = n;
				first = prev;
				continue;
			}
			if(n == null) {
				continue;
			}
			last = n;
			if (OsmMapUtils.ray_intersect_lon(prev,
					n, latitude, longitude) != -360.0d) {
				intersections++;
			}
			prev = n;
		}
		if(first == null || last == null) {
			return 0;
		}
		// special handling, also count first and last, might not be closed, but
		// we want this!
		if (ray_intersect_lon(first,
				last, latitude, longitude) != -360.0d) {
			intersections++;
		}
		return intersections;
	}

	public static int get31TileNumberX(double longitude) {
		longitude = checkLongitude(longitude);
		long l = 1L << 31;
		return (int) ((longitude + 180d)/360d * l);
	}

	public static int get31TileNumberY(double latitude) {
		latitude = checkLatitude(latitude);
		double eval = Math.log(Math.tan(toRadians(latitude)) + 1/Math.cos(toRadians(latitude)));
		long l = 1L << 31;
		if (eval > Math.PI) {
			eval = Math.PI;
		}
		return (int) ((1 - eval / Math.PI) / 2 * l);
	}



}
