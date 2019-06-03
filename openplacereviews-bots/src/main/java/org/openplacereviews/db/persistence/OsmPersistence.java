package org.openplacereviews.db.persistence;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OsmPersistence {
	private String id;
	private int timestamp;
	private boolean deployed;
}
