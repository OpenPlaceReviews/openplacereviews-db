package org.openplacereviews.osm.parser;

import org.kxml2.io.KXmlParser;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.model.*;
import org.openplacereviews.osm.model.Entity.*;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static org.openplacereviews.osm.model.DiffEntity.*;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_REF;
import static org.openplacereviews.osm.model.Entity.ATTR_TAG;
import static org.openplacereviews.osm.model.Entity.ATTR_TAG_K;
import static org.openplacereviews.osm.model.Entity.ATTR_TAG_V;
import static org.openplacereviews.osm.model.EntityInfo.*;
import static org.openplacereviews.osm.model.Relation.*;
import static org.openplacereviews.osm.model.Way.ATTR_ND;

public class OsmParser {

	private KXmlParser parser;
	private int event;

	public OsmParser(Reader reader) throws XmlPullParserException, IOException {
		parser = new KXmlParser();
		parser.setInput(reader);
		// we can skip first tag cause we don't process start document
		event = parser.next();
	}

	public OsmParser(File file) throws IOException, XmlPullParserException {
		this(new FileReader(file));
	}
	
	public boolean hasNext() {
		return event != XmlPullParser.END_DOCUMENT;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> parseNextCoordinatePlaces(long limit, Class<T> cl) throws IOException, XmlPullParserException {
		int counter = 0;
		Entity entity = null;
		DiffEntity diffEntity = null;
		boolean old = false;
		String actionType = "";
		boolean parseDiff = DiffEntity.class.equals(cl);
		List<T> results = new ArrayList<>();
		while ((event = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (event == XmlPullParser.START_TAG) {
				String elementName = parser.getName();
				long id = OUtils.parseLongSilently(getAttributeValue(ATTR_ID), -1);
				if (TAG_REMARK.equals(elementName)) {
					throw new IOException("Overpass error: " + parser.getText());
				} else if (TAG_ACTION.equals(elementName)) {
					actionType = getAttributeValue(ATTR_TYPE);
					if (ATTR_TYPE_MODIFY.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.MODIFY);
					} else if (ATTR_TYPE_CREATE.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.CREATE);
					} else if (ATTR_TYPE_DELETE.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.DELETE);
					}
				} else if (TAG_OLD.equals(elementName)) {
					// diff
					old = true;
				} else if (TAG_NEW.equals(elementName)) {
					// diff
					old = false;
				} if (EntityType.NODE.getName().equals(elementName)) {
					double lat = Double.valueOf(getAttributeValue(ATTR_LATITUDE));
					double lon = Double.valueOf(getAttributeValue(ATTR_LONGITUDE));
					entity = new Node(lat, lon, id);
					parseEntityInfo(entity);
				} else if (EntityType.WAY.getName().equals(elementName)) {
					entity = new Way(id);
					parseEntityInfo(entity);
				} else if (EntityType.RELATION.getName().equals(elementName)) {
					entity = new Relation(id);
					parseEntityInfo(entity);
				} else if (ATTR_MEMBER.equals(elementName)) {
					entity = new Relation(id);
					long ref = OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1);
					String tp = getAttributeValue(ATTR_TYPE);
					String role = getAttributeValue(ATTR_ROLE);
					((Relation)entity).addMember(ref, EntityType.valueOf(tp.toUpperCase()), role);
				} else if (ATTR_ND.equals(elementName)) {
					long lid = OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1);
					double lat = Double.valueOf(getAttributeValue(ATTR_LATITUDE));
					double lon = Double.valueOf(getAttributeValue(ATTR_LONGITUDE));
					((Way) entity).addNode(new Node(lat, lon, lid));
				} else if (ATTR_TAG.equals(elementName)) {
					String k = getAttributeValue(ATTR_TAG_K);
					String v = getAttributeValue(ATTR_TAG_V);
					entity.putTag(k, v);
				}
			} else if (event == XmlPullParser.END_TAG) {
				// here we could close the tag
				String elementName = parser.getName();
				if (parseDiff && TAG_ACTION.equals(elementName)) {
					results.add((T) diffEntity);
					diffEntity = null;
					if (limit == ++counter) {
						break;
					}
				} else if (  (EntityType.NODE.getName().equals(elementName) || 
								EntityType.WAY.getName().equals(elementName) || 
								EntityType.RELATION.getName().equals(elementName))) {
					if(!parseDiff) {
						results.add((T) entity);
						entity = null;
						if (limit == ++counter) {
							break;
						}
					} else {
						if(old) {
							diffEntity.setOldEntity(entity);
						} else {
							diffEntity.setNewEntity(entity);
						}
					}
				}
			}
		};
		return results;
	}

	private void parseEntityInfo(Entity entity) {
		EntityInfo currentParsedEntity = new EntityInfo()
				.setVersion(getAttributeValue(ATTR_VERSION))
				.setTimestamp(getAttributeValue(ATTR_TIMESTAMP))
				.setChangeset(getAttributeValue(ATTR_CHANGESET))
				.setUid(getAttributeValue(ATTR_UID))
				.setUser(getAttributeValue(ATTR_USER))
				.setVisible(getAttributeValue(ATTR_VISIBLE))
				.setAction(getAttributeValue(ATTR_ACTION));
		entity.setEntityInfo(currentParsedEntity);
	}

	protected String getAttributeValue(String atrName) {
		return parser.getAttributeValue(null, atrName);
	}

	

}
