package org.openplacereviews.scheduled;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.service.OprPlaceManager;
import org.openplacereviews.opendb.scheduled.OpenDBScheduledServices;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.openplacereviews.osm.model.Node;
import org.openplacereviews.osm.model.Way;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static org.openplacereviews.db.OpenPlaceReviewsDbBoot.OPENDB_STORAGE_REPORTS_PREF;
import static org.openplacereviews.osm.model.Entity.*;
import static org.openplacereviews.osm.model.EntityInfo.*;
import static org.openplacereviews.osm.model.Way.ATTR_ND;

@Component
@Primary
public class OpenPlaceReviewsScheduledService extends OpenDBScheduledServices {

	private static final Log LOGGER = LogFactory.getLog(OpenPlaceReviewsScheduledService.class);

	public final static String GEOJSON_FILE_NAME = "geo-location.json";
	public final static String OSM_FILE_NAME = "object-location.osm";

	@Autowired
	private OprPlaceManager oprPlaceManager;

	private File mainDirectory;
	private boolean enabled = false;

	private Gson geoJson;

	public OpenPlaceReviewsScheduledService() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}

	public void init() {
		if(getDirectory().length() > 0) {
			mainDirectory = new File(getDirectory());
			mainDirectory.mkdirs();
		}
		if(mainDirectory.exists()) {
			enabled = true;
		}
	}

	private String getDirectory() {
		return OPENDB_STORAGE_REPORTS_PREF.get();
	}

	public boolean generateFileReport(String filename) throws IOException, TransformerException, ParserConfigurationException {
		switch (filename) {
			case GEOJSON_FILE_NAME: {
				generateDailyGeoJsonReport();
				return true;
			}
			case OSM_FILE_NAME : {
				generateDailyOsmReport();
				return true;
			}
			default: {
				return false;
			}
		}
	}

	@Scheduled(fixedRate = DAY, initialDelay = MINUTE)
	public void generateDailyGeoJsonReport() throws IOException {
		if (enabled) {
			LOGGER.info("Start generating GeoJson Report ...");

			File f = new File(mainDirectory, GEOJSON_FILE_NAME);
			try (FileWriter file = new FileWriter(f)) {
				file.write(geoJson.toJson(oprPlaceManager.getAllIds()));
				file.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}

			LOGGER.info("Generating GeoJson Report is finished");
		}
	}

	@Scheduled(fixedRate = DAY, initialDelay = MINUTE)
	public void generateDailyOsmReport() throws IOException, TransformerException, ParserConfigurationException {
		if (enabled) {
			LOGGER.info("Start generating .osm Report ...");

			generateOsmXmlReport(oprPlaceManager.getOsmObjects());

			LOGGER.info("Generating .osm Report is finished");
		}
	}

	private void generateOsmXmlReport(Collection<Entity> entityList) throws ParserConfigurationException, TransformerException {
		DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();

		DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();

		Document document = documentBuilder.newDocument();
		// root element
		Element root = document.createElement("osm");
		Attr attrDate = document.createAttribute("date");
		attrDate.setValue(new Date().toString());
		root.setAttributeNode(attrDate);
		document.appendChild(root);

		// employee element
		for (Entity entity : entityList) {
			if (entity instanceof Node) {
				Element node = document.createElement("node");
				root.appendChild(node);

				// set an attribute to staff element
				generateEntityInfo(document, entity, node);
				Attr attrId = document.createAttribute(ATTR_ID);
				attrId.setValue(String.valueOf(entity.getId()));
				node.setAttributeNode(attrId);
				Attr attrLat = document.createAttribute(ATTR_LATITUDE);
				attrLat.setValue(String.valueOf(entity.getLatitude()));
				node.setAttributeNode(attrLat);
				Attr attrLon = document.createAttribute(ATTR_LONGITUDE);
				attrLon.setValue(String.valueOf(entity.getLongitude()));
				node.setAttributeNode(attrLon);


				generateTags(document, entity, node);
			} else if (entity instanceof Way) {
				Way wayElement = (Way) entity;
				Element way = document.createElement("way");
				root.appendChild(way);

				generateEntityInfo(document, entity, way);
				Attr attrId = document.createAttribute(ATTR_ID);
				attrId.setValue(String.valueOf(wayElement.getId()));
				way.setAttributeNode(attrId);

				Attr attrLat = document.createAttribute(ATTR_LATITUDE);
				attrLat.setValue(String.valueOf(entity.getLatitude()));
				way.setAttributeNode(attrLat);
				Attr attrLon = document.createAttribute(ATTR_LONGITUDE);
				attrLon.setValue(String.valueOf(entity.getLongitude()));
				way.setAttributeNode(attrLon);

				for (Node node : wayElement.getNodes()) {
					Element ndElement = document.createElement(ATTR_ND);
					Attr refNode = document.createAttribute(ATTR_REF);
					refNode.setValue(String.valueOf(node.getId()));
					ndElement.setAttributeNode(refNode);
					way.appendChild(ndElement);
				}

				generateTags(document, wayElement, way);
			}
		}

		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource domSource = new DOMSource(document);
		File f = new File(mainDirectory, OSM_FILE_NAME);
		StreamResult streamResult = new StreamResult(f);

		transformer.transform(domSource, streamResult);
	}

	private void generateTags(Document document, Entity entity, Element node) {
		for (Map.Entry<String, String> entry : entity.getTags().entrySet()) {
			Element tag = document.createElement(ATTR_TAG);
			Attr k = document.createAttribute(ATTR_TAG_K);
			k.setValue(entry.getKey());
			tag.setAttributeNode(k);
			Attr v = document.createAttribute(ATTR_TAG_V);
			v.setValue(entry.getValue());
			tag.setAttributeNode(v);

			node.appendChild(tag);
		}
	}

	private void generateEntityInfo(Document document, Entity entity, Element way) {
		if (entity.getEntityInfo() != null) {
			EntityInfo entityInfo = entity.getEntityInfo();
			if (entityInfo.getVersion() != null) {
				Attr attrVersion = document.createAttribute(ATTR_VERSION);
				attrVersion.setValue(String.valueOf(entityInfo.getVersion()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getUid() != null) {
				Attr attrVersion = document.createAttribute(ATTR_UID);
				attrVersion.setValue(String.valueOf(entityInfo.getUid()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getTimestamp() != null) {
				Attr attrVersion = document.createAttribute(ATTR_TIMESTAMP);
				attrVersion.setValue(String.valueOf(entityInfo.getTimestamp()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getVisible() != null) {
				Attr attrVersion = document.createAttribute(ATTR_VISIBLE);
				attrVersion.setValue(String.valueOf(entityInfo.getVisible()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getAction() != null) {
				Attr attrVersion = document.createAttribute(ATTR_ACTION);
				attrVersion.setValue(String.valueOf(entityInfo.getAction()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getChangeset() != null) {
				Attr attrVersion = document.createAttribute(ATTR_CHANGESET);
				attrVersion.setValue(String.valueOf(entityInfo.getChangeset()));
				way.setAttributeNode(attrVersion);
			}
			if (entityInfo.getUser() != null) {
				Attr attrVersion = document.createAttribute(ATTR_USER);
				attrVersion.setValue(String.valueOf(entityInfo.getUser()));
				way.setAttributeNode(attrVersion);
			}
		}
	}


}
