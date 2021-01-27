package org.openplacereviews.api;



import org.openplacereviews.api.OprMapCollectionApiResult.MapCollectionParameters;

public class OprPlaceDataProvider extends BaseOprPlaceDataProvider {


	@Override
	public OprMapCollectionApiResult getContent(MapCollectionParameters params) {
		OprMapCollectionApiResult m = new OprMapCollectionApiResult();
		m.parameters.put(OprMapCollectionApiResult.PARAM_TILE_BASED_KEY, true);
		m.parameters.put(OprMapCollectionApiResult.PARAM_PLACE_FILTER, placeTypes());
		if (params.tileId != null) {
			fetchObjectsByTileId(formatTile(params.tileId), m.geo);
		}
		return m;
	}
	
}
