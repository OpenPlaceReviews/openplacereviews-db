package org.openplacereviews.db.parser;

import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.openplacereviews.db.model.OsmId;
import org.openplacereviews.db.model.OsmTag;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class OsmParserKxmlImpl extends OsmParser {

	public OsmParserKxmlImpl(Reader reader) throws XmlPullParserException {
		super(reader);
	}

	public OsmParserKxmlImpl(File file) throws FileNotFoundException, XmlPullParserException {
		super(file);
	}

	@Override
	public List<OsmCoordinatePlace> parseNextCoordinatePalaces(int limit)
			throws IOException, XmlPullParserException {
		int counter = 0;
		List<OsmCoordinatePlace> result = new ArrayList<>(limit);
		OsmCoordinatePlace osmCoordinatePlace = null;
		List<OsmTag> osmTags = null;
		while (true) {
			int event = parser.next();

			if (event == XmlPullParser.START_TAG) {
				String elementName = parser.getName();
				if (elementName.equals("node")) {
					//Init osm place DTO
					osmCoordinatePlace = new OsmCoordinatePlace();
					osmTags = new ArrayList<>();
					osmCoordinatePlace.setTags(osmTags);

					mapNodeElement(osmCoordinatePlace);
					result.add(osmCoordinatePlace);
					counter += 1;
				} else if (elementName.equals("tag")) {
					OsmTag osmTag = new OsmTag();
					mapTagElement(osmTag);
					osmTags.add(osmTag);

				}
			} else if (event == XmlPullParser.END_TAG) {
				String elementName = parser.getName();
				if (elementName.equals("node")) {
					if (limit == counter) {
						return result;
					}
				}
			} else if (event == XmlPullParser.END_DOCUMENT) {
				return result;
			}
		}
	}

	protected void mapNodeElement(OsmCoordinatePlace osmCoordinatePlace) {
		String id = getAttributeValue("id");
		Double lat = Double.valueOf(getAttributeValue("lat"));
		Double lon = Double.valueOf(getAttributeValue("lon"));

		int timestamp = (int)Instant.parse(getAttributeValue("timestamp")).getEpochSecond();

		osmCoordinatePlace.setOsmId(new OsmId(id, timestamp));
		osmCoordinatePlace.setLat(lat);
		osmCoordinatePlace.setLon(lon);
	}

	protected void mapTagElement(OsmTag osmTag) {
		String k = getAttributeValue("k");
		String v = getAttributeValue("v");
		osmTag.setK(k);
		osmTag.setV(v);
	}

	protected String getAttributeValue(String atrName) {
		return parser.getAttributeValue(null, atrName);
	}

}
