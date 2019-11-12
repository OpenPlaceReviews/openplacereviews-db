package org.openplacereviews.osm.service;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.openplacereviews.opendb.ops.OpObject.*;

public class TripAdvisorBot extends GenericMultiThreadBot<TripAdvisorBot> {

	private static final String OPR_PLACE = "opr.place";
	private static final String STARS = "stars";
	private static final String REVIEWS = "reviews";
	private static final String SOURCE_TRIP_ADVISOR = "source.tripAdvisor[%s].";
	private static final String URL_PREFIX = "https://www.tripadvisor.com/";

	@Autowired
	private BlocksManager blocksManager;

	private int totalCnt = 1;
	private int progress = 0;
	private int changed = 0;

	public TripAdvisorBot(String id) {
		super(id);
	}

	@Override
	public String getTaskDescription() {
		return "Synchronising tripadvisor reviews";
	}

	@Override
	public String getTaskName() {
		return id;
	}

	@Override
	public TripAdvisorBot call() throws Exception {
		totalCnt = 1;
		changed = 0;
		progress = 0;
		addNewBotStat();
		try {
			info("Synchronization of 'tripadvisor' has started");
			OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
			blocksManager.getBlockchain().fetchAllObjects(OPR_PLACE, r);
			totalCnt = r.result.size();
			OpOperation op = initOpOperation(OPR_PLACE);
			for (OpObject opObject : r.result) {
				List<Map<String, Object>> sourcesObj = opObject.getField(null, "source", "tripAdvisor");
				if (sourcesObj != null) {
					OpObject opObjectEditOp = new OpObject();
					opObjectEditOp.putObjectValue(F_ID, opObject.getId());
					for (int i = 0; i < sourcesObj.size(); i++) {
						Map<String, Object> objectMap = sourcesObj.get(i);
						List<String> tripadvisorId = (List<String>) objectMap.get(F_ID);
						String url = URL_PREFIX + tripadvisorId.get(0) + "-" + tripadvisorId.get(1);

						Double stars = 1D;
						Integer reviews = 1;

						// TODO load from tripadvisor info about stars and reviews;

//						opObjectEditOp = generateOpObjectEditOp(opObjectEditOp, objectMap, stars, reviews, i);
						changed++;
					}
//					op = addEditOpObject(op, opObjectEditOp);
				}
				progress++;
				if (progress % 5000 == 0) {
					info(String.format("Progress of 'tripadvisor' %d / %d  (changed %d).",
							progress, totalCnt, changed));
				}
			}
//			addOpIfNeeded(op, true);
			info(String.format("Synchronization of 'tripadvisor' has finished. Scanned %d, changed %d",
					totalCnt, changed));
			setSuccessState();
		} catch (Exception e) {
			setFailedState();
			info("Synchronization  of 'tripadvisor' has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			super.shutdown();
		}
		return this;
	}

	private OpObject generateOpObjectEditOp(OpObject editObject, Map<String, Object> oldValue, Double stars, Integer reviews, Integer tripAdvisorIndex) {
		Map<String, Object> changeTagMap = editObject.getField(new TreeMap<>(), F_CHANGE);
		Map<String, Object> currentTagMap = editObject.getField(new TreeMap<>(), F_CURRENT);
		TreeMap<String, Object> setStarsMap = new TreeMap<>();
		setStarsMap.put("set", stars);
		TreeMap<String, Object> setReviewsMap = new TreeMap<>();
		setReviewsMap.put("set", reviews);
		String tripAdvisorString = String.format(SOURCE_TRIP_ADVISOR, tripAdvisorIndex);
		changeTagMap.put(tripAdvisorString + STARS, setStarsMap);
		changeTagMap.put(tripAdvisorString + REVIEWS, setReviewsMap);
		if (oldValue.size() > 1) {
			currentTagMap.put(tripAdvisorString + STARS, oldValue.get(STARS));
			currentTagMap.put(tripAdvisorString + REVIEWS, oldValue.get(REVIEWS));
		}
		if (!changeTagMap.isEmpty()) {
			editObject.putObjectValue(F_CHANGE, changeTagMap);
			editObject.putObjectValue(F_CURRENT, currentTagMap);
		}
		return editObject;
	}

	private OpOperation addEditOpObject(OpOperation op, OpObject editObject) {
		op.addEdited(editObject);
		return op;
	}

	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}
}
