package org.openplacereviews.osm.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO integrate into system
public abstract class PlaceManager {

	// 1. contains cache PlaceManager -> Super cache [ PlaceID <- [extid, exttypeid, source ] ]
	// 2. save, update, delete objects by this class for apply changes for cache
	// 3. load state cache on start app

	private Map<List<String>, Object> cache = new HashMap<>();

	public abstract void saveOperation();
	public abstract void checkObject();
	public abstract void initCache();
}
