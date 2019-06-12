package org.openplacereviews.db.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.openplacereviews.db.model.OsmId;
import org.openplacereviews.db.model.OsmTag;
import org.openplacereviews.db.tool.OsmLocationTool;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class OsmCoordinatePlacesDto {
	private String type;
	@JsonProperty("create")
	private List<OsmCoordinatePlaceDto> osmCoordinatePlacesDto;

	public OsmCoordinatePlacesDto(String type, List<OsmCoordinatePlace> osmCoordinatePlaces) {
		this.type = type;
		this.osmCoordinatePlacesDto = osmCoordinatePlaces.stream().map(osmPlace -> {
			String id = OsmLocationTool.generateStrId(osmPlace.getLat(), osmPlace.getLon());
			List<OsmId> osmIds = Arrays.asList(osmPlace.getOsmId());
			List<OsmTag> tags = osmPlace.getTags();

			return new OsmCoordinatePlaceDto(Arrays.asList(id), osmIds, tags);
		}).collect(Collectors.toList());
	}
}
