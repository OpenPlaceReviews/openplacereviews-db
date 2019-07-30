package org.openplacereviews.osm;

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

	private Reader reader;
	private KXmlParser parser;

	public OsmParser(Reader reader) throws XmlPullParserException {
		this.reader = reader;
		parser = new KXmlParser();
		parser.setInput(reader);
	}

	public OsmParser(File file) throws IOException, XmlPullParserException {
		this(new FileReader(file));
	}

	public List<Object> parseNextCoordinatePlaces(int limit, boolean parseDiff)
			throws IOException, XmlPullParserException {
		int counter = 0;
		int event ;
		Entity entity = null;
		DiffEntity diffEntity = null;
		String actionSubTag = "";
		String actionType = "";
		List<Object> results = new ArrayList<>();
		while ((event = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (event == XmlPullParser.START_TAG) {
				String elementName = parser.getName();
				long id = OUtils.parseLongSilently(getAttributeValue(ATTR_ID), -1);
				if (TAG_ACTION.equals(elementName)) {
					actionType = getAttributeValue(ATTR_TYPE);
					if (ATTR_TYPE_MODIFY.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.MODIFY);
					} else if (ATTR_TYPE_CREATE.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.CREATE);
					} else if (ATTR_TYPE_DELETE.equals(actionType)) {
						diffEntity = new DiffEntity(DiffEntity.DiffEntityType.DELETE);
					}
				} else if (TAG_OLD.equals(elementName)) {
					actionSubTag = TAG_OLD;
				} else if (TAG_NEW.equals(elementName)) {
					actionSubTag = TAG_NEW;
				}
				if (elementName.equals(EntityType.NODE.getName())) {
					double lat = Double.valueOf(getAttributeValue(ATTR_LATITUDE));
					double lon = Double.valueOf(getAttributeValue(ATTR_LONGITUDE));
					entity = new Node(lat, lon, id);
					EntityInfo currentParsedEntity = new EntityInfo()
							.setVersion(getAttributeValue(ATTR_VERSION))
							.setTimestamp(getAttributeValue(ATTR_TIMESTAMP))
							.setChangeset(getAttributeValue(ATTR_CHANGESET))
							.setUid(getAttributeValue(ATTR_UID))
							.setUser(getAttributeValue(ATTR_USER))
							.setVisible(getAttributeValue(ATTR_VISIBLE))
							.setAction(getAttributeValue(ATTR_ACTION));
					entity.setEntityInfo(currentParsedEntity);
					if (parseDiff && TAG_OLD.equals(actionSubTag)) {
						diffEntity.setOldNode(entity);
					} else if (parseDiff && (TAG_NEW.equals(actionSubTag) || actionType.equals(ATTR_TYPE_CREATE))) {
						diffEntity.setNewNode(entity);
					}
				} else if (elementName.equals(EntityType.WAY.getName())) {
					entity = new Way(id);
				} else if (elementName.equals(EntityType.RELATION.getName())) {
					entity = new Relation(id);
				} else if (elementName.equals(ATTR_MEMBER)) {
					entity = new Relation(id);
					long ref = OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1);
					String tp = getAttributeValue(ATTR_TYPE);
					String role = getAttributeValue(ATTR_ROLE);
					((Relation)entity).addMember(ref, EntityType.valueOf(tp.toUpperCase()), role);
				} else if (elementName.equals(ATTR_ND)) {
					((Way) entity).addNode(OUtils.parseLongSilently(getAttributeValue(ATTR_REF), -1));
				} else if (elementName.equals(ATTR_TAG)) {
					String k = getAttributeValue(ATTR_TAG_K);
					String v = getAttributeValue(ATTR_TAG_V);
					if (!parseDiff) {
						entity.putTag(k, v);
					} else {
						if (TAG_OLD.equals(actionSubTag)) {
							diffEntity.getOldNode().putTag(k, v);
						} else if (TAG_NEW.equals(actionSubTag) || actionType.equals(ATTR_TYPE_CREATE)) {
							diffEntity.getNewNode().putTag(k, v);
						}
					}
				}
			} else if (event == XmlPullParser.END_TAG) {
				// here we could close the tag
				String elementName = parser.getName();
				if (parseDiff && elementName.equals(TAG_ACTION)) {
					results.add(diffEntity);
					diffEntity = null;
					actionSubTag = "";
					if (limit == counter++) {
						break;
					}
				} else if (!parseDiff && (elementName.equals(EntityType.NODE.getName()) || elementName.equals(EntityType.WAY.getName()) || elementName.equals(EntityType.RELATION.getName()))) {
					results.add(entity);
					entity = null;
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