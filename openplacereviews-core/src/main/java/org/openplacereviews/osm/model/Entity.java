package org.openplacereviews.osm.model;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.openplacereviews.opendb.util.OUtils;


public abstract class Entity {
	public enum EntityType {
		NODE,
		WAY,
		RELATION,
		WAY_BOUNDARY;

		public static EntityType valueOf(Entity e) {
			if (e instanceof Node) {
				return NODE;
			} else if (e instanceof Way) {
				return WAY;
			} else if (e instanceof Relation) {
				return RELATION;
			}
			return null;
		}

		public String getName() {
			switch (this) {
				case NODE: {
					return NODE.name().toLowerCase();
				}
				case RELATION: {
					return RELATION.name().toLowerCase();
				}
				case WAY: {
					return WAY.name().toLowerCase();
				}
				case WAY_BOUNDARY: {
					return WAY_BOUNDARY.name().toLowerCase();
				}
				default: {
					return null;
				}
			}
		}
	}

	public static class EntityId {
		private final EntityType type;
		private final Long id;


		public EntityId(EntityType type, Long id) {
			this.type = type;
			this.id = id;
		}

		public static EntityId valueOf(Entity e) {
			return new EntityId(EntityType.valueOf(e), e.getId());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
		}

		@Override
		public String toString() {
			return type + " " + id; //$NON-NLS-1$
		}

		public EntityType getType() {
			return type;
		}

		public Long getId() {
			return id;
		}

		public String getOsmUrl() {
			final String browseUrl = "https://www.openstreetmap.org/browse/";
			if (type == EntityType.NODE)
				return browseUrl + "node/" + id;
			if (type == EntityType.WAY)
				return browseUrl + "way/" + id;
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			EntityId other = (EntityId) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			return true;
		}

	}

	public static final String ATTR_ID = "id";
	public static final String ATTR_LATITUDE = "lat";
	public static final String ATTR_LONGITUDE = "lon";
	public static final String ATTR_TAG = "tag";
	public static final String ATTR_TAG_K = "k";
	public static final String ATTR_TAG_V = "v";
	public static final String ATTR_REF = "ref";

	// lazy initializing
	private Map<String, String> tags = null;
	private Set<String> changedTags;
	private final long id;
	private boolean dataLoaded;
	private int modify;
	private int version;
	private double latitude;
	private double longitude;
	private EntityInfo entityInfo = null;
	public static final int MODIFY_UNKNOWN = 0;
	public static final int MODIFY_DELETED = -1;
	public static final int MODIFY_MODIFIED = 1;
	public static final int MODIFY_CREATED = 2;

	public Entity(long id) {
		this.id = id;
	}

	public Entity(long id, double latitude, double longitude) {
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Entity(Entity copy, long id) {
		this.id = id;
		for (String t : copy.getTagKeySet()) {
			putTagNoLC(t, copy.getTag(t));
		}
		this.dataLoaded = copy.dataLoaded;
		this.latitude = copy.latitude;
		this.longitude = copy.longitude;
	}

	public Set<String> getChangedTags() {
		return changedTags;
	}

	public void setChangedTags(Set<String> changedTags) {
		this.changedTags = changedTags;
	}

	public int getModify() {
		return modify;
	}

	public void setModify(int modify) {
		this.modify = modify;
	}

	public long getId() {
		return id;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String removeTag(String key) {
		if (tags != null) {
			return tags.remove(key);
		}
		return null;
	}

	public void removeTags(String... keys) {
		if (tags != null) {
			for (String key : keys) {
				tags.remove(key);
			}
		}
	}

	public String putTag(String key, String value) {
		return putTagNoLC(key.toLowerCase(), value);
	}
	
	public String putTagNoLC(String key, String value) {
		if (tags == null) {
			tags = new LinkedHashMap<String, String>();
		}
		return tags.put(key, value);
	}

	public void replaceTags(Map<String, String> toPut) {
		tags = new LinkedHashMap<String, String>(toPut);
	}

	public String getTag(String key) {
		if (tags == null) {
			return null;
		}
		return tags.get(key);
	}

	public Map<String, String> getNameTags() {
		Map<String, String> result = new LinkedHashMap<String, String>();
		for (Map.Entry<String, String> e : tags.entrySet()) {
			if (e.getKey().startsWith("name:")) {
				result.put(e.getKey(), e.getValue());
			}
		}
		return result;
	}

	public EntityInfo getEntityInfo() {
		return entityInfo;
	}

	public void setEntityInfo(EntityInfo entityInfo) {
		this.entityInfo = entityInfo;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Map<String, String> getTags() {
		if (tags == null) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(tags);
	}


	public Collection<String> getTagKeySet() {
		if (tags == null) {
			return Collections.emptyList();
		}
		return tags.keySet();
	}

	public abstract void initializeLinks(Map<EntityId, Entity> entities);


	/**
	 * @return middle point for entity
	 */
	public abstract LatLon getLatLon();


	public boolean isVirtual() {
		return id < 0;
	}

	public String getOsmUrl() {
		return EntityId.valueOf(this).getOsmUrl();
	}

	@Override
	public String toString() {
		return EntityId.valueOf(this).toString();
	}

	@Override
	public int hashCode() {
		if (id < 0) {
			return System.identityHashCode(this);
		}
		return (int) id;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity other = (Entity) obj;
		if (id != other.id)
			return false;
		// virtual are not equal
		if (id < 0) {
			return false;
		}
		return true;
	}

	public void entityDataLoaded() {
		this.dataLoaded = true;
	}

	public boolean isDataLoaded() {
		return dataLoaded;
	}

	public Map<String, String> getModifiableTags() {
		if (tags == null) {
			return Collections.emptyMap();
		}
		return tags;
	}

	public boolean compareEntity(Entity thatObj) {
		if (this == thatObj) {
			return true;
		} else {
			return this.id == thatObj.id &&
					Math.abs(latitude - thatObj.latitude) < 0.00001 &&
					Math.abs(longitude - thatObj.longitude) < 0.00001 &&
					OUtils.equals(this.tags, thatObj.tags);
		}
	}
}
