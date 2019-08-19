package org.openplacereviews.osm.model;

public class DiffEntity {

	public static String TAG_ACTION = "action";
	public static String TAG_OLD = "old";
	public static String TAG_NEW = "new";

	public static String ATTR_TYPE_MODIFY = "modify";
	public static String ATTR_TYPE_CREATE = "create";
	public static String ATTR_TYPE_DELETE = "delete";


	public enum DiffEntityType {
		MODIFY,
		CREATE,
		DELETE
	}

	private DiffEntityType type;
	private Entity newEntity;
	private Entity oldEntity;

	public DiffEntity(DiffEntityType type) {
		this.type = type;
	}

	public DiffEntityType getType() {
		return type;
	}

	public void setType(DiffEntityType type) {
		this.type = type;
	}

	public Entity getNewEntity() {
		return newEntity;
	}

	public void setNewEntity(Entity newNode) {
		this.newEntity = newNode;
	}

	public Entity getOldEntity() {
		return oldEntity;
	}

	public void setOldEntity(Entity oldNode) {
		this.oldEntity = oldNode;
	}
}
