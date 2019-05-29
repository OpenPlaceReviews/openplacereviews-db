package org.openplacereviews.db.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OsmCoordinatePlace {
	private OsmId osmId;
	private Double lat;
	private Double lon;
	private List<OsmTag> tags;
}
