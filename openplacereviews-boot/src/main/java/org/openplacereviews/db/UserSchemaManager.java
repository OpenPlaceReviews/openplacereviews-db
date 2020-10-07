package org.openplacereviews.db;


import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.INDEXED;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.NOT_INDEXED;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.DBSchemaHelper;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

@Service
public class UserSchemaManager {

	protected static final Log LOGGER = LogFactory.getLog(UserSchemaManager.class);
	private static final int USER_SCHEMA_VERSION = 0;
	public static final long EMAIL_TOKEN_EXPIRATION_TIME = 24 * 60 * 60 * 1000l;
	
	// //////////SYSTEM TABLES DDL ////////////
	private static final String SETTING_VERSION_KEY = "version";
	protected static final String SETTINGS_TABLE = "user_settings";
	protected static final String USERS_TABLE = "users";

	private static DBSchemaHelper dbschema = new DBSchemaHelper(SETTINGS_TABLE);
	protected static final int BATCH_SIZE = 1000;
	
	@Value("${spring.userdatasource.url}")
	private String userDataSourceUrl;
	
	@Value("${spring.userdatasource.username}")
	private String userDataSourceUsername;
	
	@Value("${spring.userdatasource.password}")
	private String userDataSourcePassword;

	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter formatter;

	
	static {
		dbschema.registerColumn(USERS_TABLE, "uid", "serial primary key", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "nickname", "text unique", INDEXED);
		dbschema.registerColumn(USERS_TABLE, "email", "text", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "emailtoken", "text", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "tokendate", "timestamp", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "sprivkey", "text", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "lprivkey", "text", NOT_INDEXED);
		dbschema.registerColumn(USERS_TABLE, "signup", "jsonb", NOT_INDEXED);
//		dbschema.registerColumn(USERS_TABLE, "loginkey", "text", NOT_INDEXED);

	}
	

	public DataSource userDataSource() {
		return DataSourceBuilder.create().url(userDataSourceUrl).username(userDataSourceUsername)
				.password(userDataSourcePassword).build();
	}


	public void initializeDatabaseSchema(MetadataDb metadataDB) {
		JdbcTemplate jdbcTemplate = getJdbcTemplate();
		dbschema.initSettingsTable(metadataDB, jdbcTemplate);
		dbschema.createTablesIfNeeded(metadataDB, jdbcTemplate);
		migrateDBSchema(jdbcTemplate);
	}
	
	public JdbcTemplate getJdbcTemplate() {
		if (jdbcTemplate == null) {
			jdbcTemplate = new JdbcTemplate(userDataSource());
		}
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

	public void createNewUser(String name, String email, String emailToken, String sprivkey, OpObject obj) {
		PGobject userObj = new PGobject();
		userObj.setType("jsonb");
		try {
			userObj.setValue(formatter.fullObjectToJson(obj));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		getJdbcTemplate().update(
				"INSERT INTO " + USERS_TABLE + "(nickname,email,emailtoken,tokendate,sprivkey,signup) VALUES(?,?,?,?,?,?)", name,
				email, emailToken, new Date(), sprivkey, userObj);

	}
	
	

	public String getSignupPrivateKey(String name) {
		return getJdbcTemplate().query("SELECT sprivkey FROM " + USERS_TABLE + " WHERE nickname = ?",
				new Object[] { name }, new ResultSetExtractor<String>() {
					@Override
					public String extractData(ResultSet arg0) throws SQLException, DataAccessException {
						if (!arg0.next()) {
							return null;
						}
						return arg0.getString(1);
					}
				});
	}
	
	public String getUserEmail(String name) {
		return getJdbcTemplate().query("SELECT email FROM " + USERS_TABLE + " WHERE nickname = ?",
				new Object[] { name }, new ResultSetExtractor<String>() {
					@Override
					public String extractData(ResultSet arg0) throws SQLException, DataAccessException {
						if (!arg0.next()) {
							return null;
						}
						return arg0.getString(1);
					}
				});
	}
	
	public String getLoginPrivateKey(String name) {
		return getJdbcTemplate().query("SELECT lprivkey FROM " + USERS_TABLE + " WHERE nickname = ?",
				new Object[] { name }, new ResultSetExtractor<String>() {
					@Override
					public String extractData(ResultSet arg0) throws SQLException, DataAccessException {
						if (!arg0.next()) {
							return null;
						}
						return arg0.getString(1);
					}
				});
	}

	public OpOperation validateEmail(String name, String token) {
		OpOperation op = getJdbcTemplate().query(
				"SELECT signup, emailtoken, tokendate FROM " + USERS_TABLE + " WHERE nickname = ?",
				new Object[] { name }, new ResultSetExtractor<OpOperation>() {

					@Override
					public OpOperation extractData(ResultSet rs) throws SQLException, DataAccessException {
						if (!rs.next()) {
							throw new IllegalStateException("User wasn't registered yet");
						}
						if (System.currentTimeMillis() - rs.getDate(3).getTime() > EMAIL_TOKEN_EXPIRATION_TIME) {
							throw new IllegalStateException("Registration link has expired");
						}
						OpOperation signupOp = formatter.parseOperation(rs.getString(1));
						if (!token.equals(rs.getString(2))) {
							throw new IllegalArgumentException("Registration token doesn't match, please signup again or reset password");
						}
						return signupOp;
					}
				});
		return op;
	}
	
	public void resetEmailToken(String name, String emailToken) {
		getJdbcTemplate().update("UPDATE " + USERS_TABLE + " SET emailtoken = ?, tokendate = ? WHERE nickname = ?", 
				emailToken, new Date(), name);

	}
	
	public void updateLoginKey(String name, String lprivkey) {
		getJdbcTemplate().update("UPDATE " + USERS_TABLE + " SET lprivkey = ?, emailtoken = null WHERE nickname = ?", lprivkey, name);
	}
	
	public void updateSignupKey(String name, String sprivkey) {
		getJdbcTemplate().update("UPDATE " + USERS_TABLE + " SET sprivkey = ?, emailtoken = null WHERE nickname = ?", sprivkey, name);
	}

}
