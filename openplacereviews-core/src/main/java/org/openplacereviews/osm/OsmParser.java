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
import org.openplacereviews.osm.model.*;
import org.openplacereviews.osm.model.Entity.EntityType;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import static org.openplacereviews.osm.model.Entity.*;
import static org.openplacereviews.osm.model.EntityInfo.*;
import static org.openplacereviews.osm.model.Relation.*;
import static org.openplacereviews.osm.model.Way.ATTR_ND;

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

	public List<Entity> parseNextCoordinatePlaces(int limit)
			throws IOException, XmlPullParserException {
		int counter = 0;
		int event ;
		Entity e = null;
		List<Entity> results = new ArrayList<Entity>();
		while ((event = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (event == XmlPullParser.START_TAG) {
				String elementName = parser.getName();
				long id = OUtils.parseLongSilently(getAttributeValue(ATTR_ID), -1);
				if (elementName.equals(EntityType.NODE.getName())) {
					double lat = Double.valueOf(getAttributeValue(ATTR_LATITUDE));
					double lon = Double.valueOf(getAttributeValue(ATTR_LONGITUDE));
					e = new Node(lat, lon, id);
					EntityInfo currentParsedEntity = new EntityInfo()
							.setVersion(getAttributeValue(ATTR_VERSION))
							.setTimestamp(getAttributeValue(ATTR_TIMESTAMP))
							.setChangeset(getAttributeValue(ATTR_CHANGESET))
							.setUid(getAttributeValue(ATTR_UID))
							.setUser(getAttributeValue(ATTR_USER))
							.setVisible(getAttributeValue(ATTR_VISIBLE))
							.setAction(getAttributeValue(ATTR_ACTION));
					e.setEntityInfo(currentParsedEntity);
				} else if (elementName.equals(EntityType.WAY.getName())) {
					e = new Way(id);
				} else if (elementName.equals(EntityType.RELATION.getName())) {
					e = new Relation(id);
				} else if (elementName.equals(ATTR_MEMBER)) {
					e = new Relation(id);
					long ref = OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1);
					String tp = getAttributeValue(ATTR_TYPE);
					String role = getAttributeValue(ATTR_ROLE);
					((Relation)e).addMember(ref, EntityType.valueOf(tp.toUpperCase()), role);
				} else if (elementName.equals(ATTR_ND)) {
					((Way) e).addNode(OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1));
				} else if (elementName.equals(ATTR_TAG)) {
					String k = getAttributeValue(ATTR_TAG_K);
					String v = getAttributeValue(ATTR_TAG_V);
					e.putTag(k, v);
				}
			} else if (event == XmlPullParser.END_TAG) {
				// here we could close the tag
				String elementName = parser.getName();
				if (elementName.equals(EntityType.NODE.toString()) || elementName.equals(EntityType.WAY.toString()) || elementName.equals(EntityType.RELATION.toString())) {
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
