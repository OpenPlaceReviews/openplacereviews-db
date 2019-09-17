package org.openplacereviews.osm.service;

import org.openplacereviews.opendb.ops.OpObject;

public class RegionImportBot extends GenericMultiThreadBot<RegionImportBot> {

	public RegionImportBot(OpObject botObject) {
		super(botObject);
	}

	private int totalCnt = 1;
	private int progress = 0;

	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}

	@Override
	public String getTaskDescription() {
		return "Synchronising region id";
	}

	@Override
	public String getTaskName() {
		return "region-sync";
	}

	@Override
	public RegionImportBot call() throws Exception {
		return null;
	}
}
