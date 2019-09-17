package org.openplacereviews.controllers;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.openplacereviews.db.service.OprPlaceManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.scheduled.OpenPlaceReviewsScheduledService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/api/places")
public class PlaceController {

	@Autowired
	public OprPlaceManager oprPlaceManager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private OpenPlaceReviewsScheduledService openPlaceReviewsScheduledService;

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

	private class ReportInfo {
		public String fileName;
		public String filePath;
		public Long size;
		public Long date;
	}

	@GetMapping(value = "/report")
	@ResponseBody
	public ResponseEntity<String> getReportFiles() throws IOException {
		File folder = new File(openPlaceReviewsScheduledService.getDirectory());
		File[] listOfFiles = folder.listFiles();

		List<ReportInfo> files = new ArrayList<>();
		if (listOfFiles != null) {
			for (File file : listOfFiles) {
				ReportInfo reportInfo = new ReportInfo();
				reportInfo.fileName = file.getName();
				reportInfo.filePath = file.getPath();
				reportInfo.size = file.length();
				BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
				reportInfo.date = attr.lastModifiedTime().toMillis();
				files.add(reportInfo);
			}
		}

		return ResponseEntity.ok(formatter.fullObjectToJson(files));
	}

	@GetMapping(value = "/report/{filename}")
	@ResponseBody
	public FileSystemResource getGeoLocationReport(@PathVariable("filename") String fileName) {
		File file = new File(openPlaceReviewsScheduledService.getDirectory(), fileName);
		return new FileSystemResource(file);
	}

	@GetMapping(path = "/geojson-by-id")
	public ResponseEntity<String> tileData(HttpSession session, 
			@RequestParam(required = false) String tileId) {
		return ResponseEntity.ok(geoJson.toJson(oprPlaceManager.getIdsByTileId(tileId)));
	}

	@PutMapping(path = "/report/{filename}")
	@ResponseBody
	public ResponseEntity<String> generateReport(@PathVariable("filename") String fileName) throws ParserConfigurationException, TransformerException, IOException {
		if (openPlaceReviewsScheduledService.generateFileReport(fileName)) {
			return ResponseEntity.ok("Report was updated");
		} else {
			return ResponseEntity.ok("Error while report updating");
		}
	}
}
