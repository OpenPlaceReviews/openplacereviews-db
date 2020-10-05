package org.openplacereviews.db;


import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.INDEXED;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.NOT_INDEXED;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.service.DBSchemaHelper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserSchemaManager {

	protected static final Log LOGGER = LogFactory.getLog(UserSchemaManager.class);
	private static final int USER_SCHEMA_VERSION = 0;
	
	// //////////SYSTEM TABLES DDL ////////////
	private static final String SETTING_VERSION_KEY = "version";
	protected static final String SETTINGS_TABLE = "user_settings";
	protected static final String USERS_TABLE = "users";

	private static DBSchemaHelper dbschema = new DBSchemaHelper(SETTINGS_TABLE);
	protected static final int BATCH_SIZE = 1000;
	private JdbcTemplate jdbcTemplate;


	static {

		dbschema.registerColumn(USERS_TABLE, "uid", "serial not null", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "nickname", "text", INDEXED);
		dbschema.registerColumn(USERS_TABLE, "email", "text", NOT_INDEXED);

	}

	public void initializeDatabaseSchema(MetadataDb metadataDB, JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		dbschema.initSettingsTable(metadataDB, jdbcTemplate);
		dbschema.createTablesIfNeeded(metadataDB, jdbcTemplate);
		migrateDBSchema(jdbcTemplate);
	}
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	private void migrateDBSchema(JdbcTemplate jdbcTemplate) {
		int dbVersion = dbschema.getIntSetting(jdbcTemplate, SETTING_VERSION_KEY);
		if(dbVersion < USER_SCHEMA_VERSION) {
			// migration is only needed for alter operation and changing data
			// migration is not needed for adding new columns
			if(dbVersion <= 1) {
				// Migrate from 0 - > 1
			}
			if(dbVersion <= 2) {
				// Migrate from 1 - > 2
			}
			dbschema.setSetting(jdbcTemplate, SETTING_VERSION_KEY, USER_SCHEMA_VERSION + "");
		} else if(dbVersion > USER_SCHEMA_VERSION) {
			throw new UnsupportedOperationException();
		}
	}

}
