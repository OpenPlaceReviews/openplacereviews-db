package org.openplacereviews.osm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.kxml2.io.KXmlParser;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.Node;
import org.openplacereviews.osm.model.Relation;
import org.openplacereviews.osm.model.Way;
import org.openplacereviews.osm.model.Entity.EntityType;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class OsmParser {

	private Reader reader;
	private KXmlParser parser;

	public OsmParser(Reader reader) throws XmlPullParserException {
		this.reader = reader;
		parser = new KXmlParser();
		parser.setInput(reader);
	}

	public OsmParser(File file) throws FileNotFoundException, XmlPullParserException {
		this(new FileReader(file));
	}

	public List<Entity> parseNextCoordinatePalaces(int limit)
			throws IOException, XmlPullParserException {
		int counter = 0;
		int event ;
		Entity e = null;
		List<Entity> results = new ArrayList<Entity>();
		while ((event = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (event == XmlPullParser.START_TAG) {
				String elementName = parser.getName();
				long id = OUtils.parseLongSilently(getAttributeValue("id"), -1);
				if (elementName.equals("node")) {
					double lat = Double.valueOf(getAttributeValue("lat"));
					double lon = Double.valueOf(getAttributeValue("lon"));
					e = new Node(lat, lon, id);
				} else if (elementName.equals("way")) {
					e = new Way(id);
				} else if (elementName.equals("relation")) {
					e = new Relation(id);
				} else if (elementName.equals("member")) {
					e = new Relation(id);
					long ref = OUtils.parseLongSilently(getAttributeValue("ref"), -1);
					String tp = getAttributeValue("type");
					String role = getAttributeValue("role");
					((Relation)e).addMember(ref, EntityType.valueOf(tp.toUpperCase()), role);
				} else if (elementName.equals("nd")) {
					((Way) e).addNode(OUtils.parseLongSilently(getAttributeValue("ref"), -1));
				} else if (elementName.equals("tag")) {
					String k = getAttributeValue("k");
					String v = getAttributeValue("v");
					e.putTag(k, v);
				}
			} else if (event == XmlPullParser.END_TAG) {
				// here we could close the tag
				String elementName = parser.getName();
				if (elementName.equals("node") || elementName.equals("way") || elementName.equals("relation")) {
					results.add(e);
					e = null;
					if (limit == counter++) {
						break;
					}
				}
			}
		}
		return results;
	}

	protected String getAttributeValue(String atrName) {
		return parser.getAttributeValue(null, atrName);
	}

}
