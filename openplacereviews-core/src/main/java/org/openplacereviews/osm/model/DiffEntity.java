package org.openplacereviews.osm.model;

public class DiffEntity {


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
