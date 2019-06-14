package org.openplacereviews.db.persistence;

import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DbPlacesService {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	/**
	 * Insert places in DB with {@code deployed} flag
	 * @param places obj
	 * @param deployed flag
	 */
	public void insertPlaces(List<OsmCoordinatePlace> places, boolean deployed) {
		for (OsmCoordinatePlace place : places) {
			String osmId = place.getOsmId().id;
			Integer timestamp = place.getOsmId().getTimestamp();
			jdbcTemplate.execute("MERGE INTO place (id, timestamp, deployed) " +
					" KEY(id) " +
					" VALUES(" + osmId + ", " + timestamp + ", " + deployed + ");");
		}

	}

	/**
	 * Check is osm place deployed to open-db
	 *
	 * @param place {@link OsmCoordinatePlace}
	 * @return
	 */
	public boolean isDeployed(OsmCoordinatePlace place) {
		int rowCount = jdbcTemplate
				.queryForObject("SELECT count(id) FROM place WHERE id=" + place.getOsmId().getId(), Integer.class);
		if (rowCount == 0) {
			long afterFilter = System.currentTimeMillis();
			return false;
		}
		OsmPersistence osmPlace = jdbcTemplate
				.queryForObject("SELECT * FROM place WHERE id=" + place.getOsmId().getId(), new OsmRowMapper());

		return osmPlace.isDeployed();
	}

	/**
	 * Checks if at least one object exists in the database.
	 * @param places
	 * @return
	 */
	public boolean areSomeExistInDb(List<OsmCoordinatePlace> places) {
		if (places == null || places.isEmpty())
			return false;

		Set<String> placesIds = places.stream().map(place -> place.getOsmId().getId()).collect(Collectors.toSet());

		MapSqlParameterSource parameters = new MapSqlParameterSource("ids", placesIds);
		int count = namedParameterJdbcTemplate
				.queryForObject("SELECT count(*) FROM place WHERE id IN (:ids)", parameters, Integer.class);

		if (count > 0) {
			return true;
		}

		return false;
	}

	/**
	 * Count amount of undeployed places from table.
	 *
	 * @return
	 */
	public int countUndeployedPlaces() {
		return jdbcTemplate.queryForObject("SELECT count(*) FROM place WHERE deployed=false", Integer.class);
	}

	private class OsmRowMapper implements RowMapper<OsmPersistence> {

		@Override
		public OsmPersistence mapRow(ResultSet rs, int rowNum) throws SQLException {
			OsmPersistence place = new OsmPersistence();
			place.setId(rs.getString("id"));
			place.setTimestamp(rs.getInt("timestamp"));
			place.setDeployed(rs.getBoolean("deployed"));

			return place;
		}
	}

}
