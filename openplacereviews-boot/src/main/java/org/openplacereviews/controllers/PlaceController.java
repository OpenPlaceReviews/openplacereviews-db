package org.openplacereviews.controllers;

import javax.servlet.http.HttpSession;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Controller
@RequestMapping("/api/places")
public class PlaceController {

	@Autowired
	public OprPlaceManager oprPlaceManager;

	@Autowired
	private JsonFormatter formatter;


	private Gson geoJson;
	
	public PlaceController() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}

	
	@GetMapping(path = "/geojson-ids")
	public ResponseEntity<String> allIds(HttpSession session) {
		return ResponseEntity.ok(geoJson.toJson(oprPlaceManager.getAllIds()));
	}

	@GetMapping(path = "/osm-objects")
	public ResponseEntity<String> osmObjects(HttpSession session) {
		return ResponseEntity.ok(formatter.fullObjectToJson(oprPlaceManager.getOsmObjects()));
	}

	@GetMapping(path = "/geojson-by-id")
	public ResponseEntity<String> tileData(HttpSession session, 
			@RequestParam(required = false) String tileId) {
		return ResponseEntity.ok(geoJson.toJson(oprPlaceManager.getIdsByTileId(tileId)));
	}

	
}
