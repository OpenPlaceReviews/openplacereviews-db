package org.openplacereviews.db.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.kxml2.io.KXmlParser;
import org.openplacereviews.db.model.OsmPlace;
import org.xmlpull.v1.XmlPullParserException;

public abstract class OsmParser {

	private final Reader reader;

	protected final KXmlParser parser;

	public OsmParser(Reader reader) throws XmlPullParserException {
		this.reader = reader;
		this.parser = new KXmlParser();
		this.parser.setInput(reader);
	}

	public OsmParser(File file) throws FileNotFoundException, XmlPullParserException {
		this(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
	}

	/**
	 * Parse next {@code limit} coordinate places elements</br>
	 * from reader in constructor
	 * @param limit
	 * @return
	 */
	public abstract List<OsmPlace> parseNextCoordinatePalaces(int limit)
			throws IOException, XmlPullParserException;
}
