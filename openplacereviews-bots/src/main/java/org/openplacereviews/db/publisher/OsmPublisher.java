package org.openplacereviews.db.publisher;

import org.openplacereviews.db.parser.OsmParser;

public interface OsmPublisher {
	/**
	 * Publish Osm places to open-db using {@link OsmParser}
	 * @param osmParser
	 */
	void publish(OsmParser osmParser);
}
