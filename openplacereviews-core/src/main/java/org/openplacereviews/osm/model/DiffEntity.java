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
	private Entity newNode;
	private Entity oldNode;

	public DiffEntity(DiffEntityType type) {
		this.type = type;
	}

	public DiffEntityType getType() {
		return type;
	}

	public void setType(DiffEntityType type) {
		this.type = type;
	}

	public Entity getNewNode() {
		return newNode;
	}

	public void setNewNode(Entity newNode) {
		this.newNode = newNode;
	}

	public Entity getOldNode() {
		return oldNode;
	}

	public void setOldNode(Entity oldNode) {
		this.oldNode = oldNode;
	}
}
