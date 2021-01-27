package org.openplacereviews.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;

public class OprMapCollectionApiResult {
	public static final String PARAM_DATE_KEY = "date";
	public static final String PARAM_DATE2_KEY = "date2";
	public static final String PARAM_TILE_BASED_KEY = "tileid";
	public static final String PARAM_PLACE_FILTER = "placeTypes";
	
	public FeatureCollection geo = new FeatureCollection(new ArrayList<Feature>());
	
	public Map<String, Object> parameters = new LinkedHashMap<>();
	
	public static class MapCollectionParameters {
		
		Date date;
		Date date2;
		String tileId;
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((date == null) ? 0 : date.hashCode());
			result = prime * result + ((date2 == null) ? 0 : date2.hashCode());
			result = prime * result + ((tileId == null) ? 0 : tileId.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MapCollectionParameters other = (MapCollectionParameters) obj;
			if (date == null) {
				if (other.date != null)
					return false;
			} else if (!date.equals(other.date))
				return false;
			if (date2 == null) {
				if (other.date2 != null)
					return false;
			} else if (!date2.equals(other.date2))
				return false;
			if (tileId == null) {
				if (other.tileId != null)
					return false;
			} else if (!tileId.equals(other.tileId))
				return false;
			return true;
		}
		
		
		
	}
	
}