package org.openplacereviews.db.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OsmTag {
	private String k;
	private String v;
}
