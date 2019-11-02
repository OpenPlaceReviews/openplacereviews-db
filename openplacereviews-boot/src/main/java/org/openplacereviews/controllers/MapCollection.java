package org.openplacereviews.controllers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;

public class MapCollection {

	public static final String TYPE_DATE = "date";
	public static final String TITLE = "title";

	public FeatureCollection geo = new FeatureCollection(new ArrayList<Feature>());
	
	public Map<String, String> parameters = new LinkedHashMap<String, String>();
	
	public boolean tileBased = false;
	
	public Map<String, String> placeTypes;
}
