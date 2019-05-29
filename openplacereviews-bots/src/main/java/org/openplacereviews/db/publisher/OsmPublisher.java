package org.openplacereviews.db.publisher;

import org.openplacereviews.db.parser.OsmParser;

public interface OsmPublisher {
	void publish(OsmParser osmParser);
}
