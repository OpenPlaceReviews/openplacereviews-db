package org.openplacereviews.osm.model;

/**
 * Additional entity info
 */
public class EntityInfo {
	public static final String ATTR_TIMESTAMP = "timestamp";
	public static final String ATTR_UID = "uid";
	public static final String ATTR_USER = "user";
	public static final String ATTR_VISIBLE = "visible";
	public static final String ATTR_VERSION = "version";
	public static final String ATTR_CHANGESET = "changeset";
	public static final String ATTR_ACTION= "action";

	private String timestamp;
	private String uid;
	private String user;
	private String visible;
	private String version;
	private String changeset;
	private String action;
	
	public EntityInfo() {
	}

	public EntityInfo(String version) {
		this.version = version;
	}
	
	
	public String getAction() {
		return action;
	}
	public EntityInfo setAction(String action) {
		this.action = action;
		return this;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public EntityInfo setTimestamp(String timestamp) {
		this.timestamp = timestamp;
		return this;
	}
	public String getUid() {
		return uid;
	}
	public EntityInfo setUid(String uid) {
		this.uid = uid;
		return this;
	}
	public String getUser() {
		return user;
	}
	public EntityInfo setUser(String user) {
		this.user = user;
		return this;
	}
	public String getVisible() {
		return visible;
	}
	public EntityInfo setVisible(String visible) {
		this.visible = visible;
		return this;
	}
	public String getVersion() {
		return version;
	}
	public EntityInfo setVersion(String version) {
		this.version = version;
		return this;
	}
	public String getChangeset() {
		return changeset;
	}
	public EntityInfo setChangeset(String changeset) {
		this.changeset = changeset;
		return this;
	}

}
