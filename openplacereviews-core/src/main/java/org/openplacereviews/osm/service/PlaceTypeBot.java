package org.openplacereviews.osm.service;


import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;
import org.springframework.beans.factory.annotation.Autowired;

public class PlaceTypeBot extends GenericMultiThreadBot<PlaceTypeBot> {

	protected static final Log LOGGER = LogFactory.getLog(PlaceTypeBot.class);

	private static final String F_BLOCK_HASH = "lastblock";
	
	@Autowired
	private BlocksManager blocksManager;

	private int totalCnt = 1;
	private int progress = 0;

	private int changed;
	
	public PlaceTypeBot(OpObject botObject) {
		super(botObject);
	}
	
	@Override
	public synchronized PlaceTypeBot call() throws Exception {
		addNewBotStat();
		super.initVars();
		try {
			OpBlockChain blc = blocksManager.getBlockchain();
			OpBlockChain init = blc;
			totalCnt = blc.countAllObjects("opr.place");
			progress = 0;
			changed = 0;
			String lastScannedBlockHash = botObject.getField(null, F_BOT_STATE, F_BLOCK_HASH);
			LOGGER.info(
					addInfoLogEntry(String.format("Synchronization of 'placetype' has started from block %s. Total %d places.", lastScannedBlockHash, totalCnt)));
			OpOperation op = initOpOperation("opr.place");
			Set<CompoundKey> keys = new HashSet<CompoundKey>();
			boolean blockExist = blc.getBlockHeaderByRawHash(wrapNull(lastScannedBlockHash)) != null;
			while (blc != null && !blc.isNullBlock()) {
				Iterator<Entry<CompoundKey, OpObject>> it = blc.getRawSuperblockObjects("opr.place").iterator();
				while (it.hasNext()) {
					Entry<CompoundKey, OpObject> e = it.next();
					if (!keys.add(e.getKey())) {
						continue;
					}
					progress++;
					OpObject o = e.getValue();
					String tp = o.getField(null, "placetype");
					String ntp = evalPlaceType(o);
					if (!OUtils.equals(wrapNull(ntp), wrapNull(tp))) {
						PlaceOpObjectHelper.generateSetOperation(op, o.getId(), "placetype", tp, ntp);
						op = addOpIfNeeded(op, false);
						changed++;
					}
					if (progress % 5000 == 0) {
						LOGGER.info(
								addInfoLogEntry(String.format("Progress of 'placetype' %d / %d  (changed %d).",
										progress, totalCnt, changed)));
					}
				}
				blc = blc.getParent();
				if (blockExist && blc.getBlockHeaderByRawHash(wrapNull(lastScannedBlockHash)) == null) {
					break;
				}
			}
			op = addOpIfNeeded(op, true);
			String lastBlockRawHash = init.getLastBlockRawHash();
			if (changed > 0 &&
					!OUtils.equals(lastBlockRawHash, lastScannedBlockHash)) {
				op = initOpOperation(botObject.getParentType());
				PlaceOpObjectHelper.generateSetOperation(op, botObject.getId(),
						F_BOT_STATE + "." + F_BLOCK_HASH, lastScannedBlockHash, lastBlockRawHash);
				addOpIfNeeded(op, true);
			}
			LOGGER.info(
					addInfoLogEntry(String.format("Synchronization of 'placetype' has finished. Scanned %d, changed %d",
							progress, changed)));
			setSuccessState();
		} catch (Exception e) {
			setfailedState();
			LOGGER.info(addErrorLogEntry("Synchronization  of 'placetype' has failed: " + e.getMessage(), e), e);
			throw e;
		} finally {
			super.shutdown();
		}
		return this;
	}
	
	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}

	private String wrapNull(String tp) {
		if(tp == null) {
			return "";
		}
		return tp;
	}

	private String evalPlaceType(OpObject o) {
		List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
		if(osmList == null || osmList.size() == 0) {
			return "";
		}
		Map<String, Object> osm = osmList.get(0);
		return (String) osm.get("osm_value");
	}

	@Override
	public String getTaskDescription() {
		return "Evaluating main tags";
	}

	@Override
	public String getTaskName() {
		return "place-type";
	}
	
}
