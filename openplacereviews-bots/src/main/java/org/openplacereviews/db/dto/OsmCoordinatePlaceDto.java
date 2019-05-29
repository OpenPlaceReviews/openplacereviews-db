package org.openplacereviews.db.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.openplacereviews.db.model.OsmId;
import org.openplacereviews.db.model.OsmTag;
import org.springframework.boot.jackson.JsonComponent;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OsmCoordinatePlaceDto {
	private  List<String> id;
	private List<OsmId> osmId;
	private List<OsmTag> tags;
}
