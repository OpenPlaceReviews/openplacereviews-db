package org.openplacereviews.db.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.persistence.OsmPersistence;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.osm.OsmLocationTool;
import org.openplacereviews.osm.model.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DbOprOsmSchemaManager implements ApplicationListener<ApplicationReadyEvent> {

	private static final String TAGS_TABLE = "tags_settings";
	private static final String PLACE_TABLE = "place";

	private static final Log LOGGER = LogFactory.getLog(DbOprOsmSchemaManager.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter jsonFormatter;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Override
	// TODO change creating tables
	public void onApplicationEvent(final ApplicationReadyEvent event) {
		LOGGER.info("Creating table " + TAGS_TABLE);

		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS " + TAGS_TABLE + "(id serial primary key, value jsonb, date timestamp)");
		LOGGER.info("Creating table " + PLACE_TABLE);
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS " + PLACE_TABLE + "(id VARCHAR(19) PRIMARY KEY, deployed BOOLEAN DEFAULT FALSE)");
		jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS pk_index ON " + PLACE_TABLE +"(id)");
	}

	public DbOprOsmSchemaManager() {
	}

	public void addNewTagInfo(Map<String, Object> tagInfo) {
		jdbcTemplate.update("INSERT INTO " + TAGS_TABLE + "(value, date) VALUES (?, ?)", jsonFormatter.fullObjectToJson(tagInfo), new Date());
	}

	private String getLastTagInfo() {
		final String[] res = new String[1];
		jdbcTemplate.query("SELECT value from " + TAGS_TABLE + " ORDER BY date DESC LIMIT 1", new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				res[0] = rs.getString(1);
			}
		});

		return res[0];
	}

	/**
	 * Insert places in DB with {@code deployed} flag
	 * @param places obj
	 * @param deployed flag
	 */
	public void insertPlaces(List<Entity> places, boolean deployed) {
		for (Entity place : places) {
			String osmId = OsmLocationTool.generateStrId(place.getLatLon());
			jdbcTemplate.execute("INSERT INTO place(id, deployed)  VALUES(\'" + osmId + "\', " + deployed + ") " +
					" ON CONFLICT (id) DO UPDATE SET deployed = " + deployed);
		}
	}

	/**
	 * Check is osm place deployed to open-db
	 *
	 * @param place {@link Entity}
	 * @return
	 */
	public boolean isDeployed(Entity place) {
		int rowCount = jdbcTemplate
				.queryForObject("SELECT count(id) FROM place WHERE id=" + OsmLocationTool.generateStrId(place.getLatLon()), Integer.class);
		if (rowCount == 0) {
			long afterFilter = System.currentTimeMillis();
			return false;
		}
		OsmPersistence osmPlace = jdbcTemplate
				.queryForObject("SELECT * FROM place WHERE id=" + OsmLocationTool.generateStrId(place.getLatLon()), new OsmRowMapper());

		return osmPlace.isDeployed();
	}

	/**
	 * Checks if at least one object exists in the database.
	 * @param places
	 * @return
	 */
	public boolean areSomeExistInDb(List<Entity> places) {
		if (places == null || places.isEmpty())
			return false;

		Set<String> placesIds = places.stream().map(place -> OsmLocationTool.generateStrId(place.getLatLon())).collect(Collectors.toSet());

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
