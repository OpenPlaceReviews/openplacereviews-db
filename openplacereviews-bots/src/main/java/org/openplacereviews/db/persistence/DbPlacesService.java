package org.openplacereviews.db.persistence;

import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Service
public class DbPlacesService {

	@Autowired
	private JdbcTemplate jdbcTemplate;


	public void insertPlaces(List<OsmCoordinatePlace> places, boolean deployed) {
		long timeBeforeQuery = System.currentTimeMillis();
		for (OsmCoordinatePlace place : places) {
			String osmId = place.getOsmId().getId();
			jdbcTemplate.execute("MERGE INTO place (id, deployed) " +
					" KEY(id) " +
					" VALUES(" + osmId +", " + deployed + ");");
		}

		long timeAfterQuery = System.currentTimeMillis();
		System.out.println("Time spent on merge: " + (timeAfterQuery - timeBeforeQuery) + " ms.");
	}

	public void select() {
//		System.out.println("Size: " + jdbcTemplate.queryForList("SELECT * FROM place").size());

	}
}
