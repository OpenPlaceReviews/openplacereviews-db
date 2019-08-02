package org.openplacereviews.osm.db;

import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

public class DbManager {

	protected static String SETTINGS_TABLE = "opendb_settings";
	protected static String SETTINGS_TABLE_SYNC = "db.sync";

	private BlocksManager blocksManager;
	private JdbcTemplate jdbcTemplate;

	public DbManager(BlocksManager blocksManager, JdbcTemplate jdbcTemplate) {
		this.blocksManager = blocksManager;
		this.jdbcTemplate = jdbcTemplate;
	}

	public boolean saveNewSyncState(TreeMap<String, Object> v) {
		String value = blocksManager.getBlockchain().getRules().getFormatter().fullObjectToJson(v);
		return jdbcTemplate.update("insert into  " + SETTINGS_TABLE + "(key,value) values (?, ?) "
				+ " ON CONFLICT (key) DO UPDATE SET value = ? ", SETTINGS_TABLE_SYNC, value, value) != 0;
	}

	public TreeMap<String, Object> getDBSyncState() {
		String s = null;
		try {
			s = jdbcTemplate.query("select value from " + SETTINGS_TABLE + " where key = ?", new ResultSetExtractor<String>() {

				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					boolean next = rs.next();
					if (next) {
						return rs.getString(1);
					}
					return null;
				}
			}, SETTINGS_TABLE_SYNC);
		} catch (DataAccessException e) {
			return null;
		}

		return blocksManager.getBlockchain().getRules().getFormatter().fromJsonToTreeMap(s);
	}
}
