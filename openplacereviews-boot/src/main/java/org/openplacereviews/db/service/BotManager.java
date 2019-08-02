package org.openplacereviews.db.service;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.osm.service.BotPlacePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.openplacereviews.osm.service.BotPlacePublisher.OP_BOT;

@Service
public class BotManager {

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private volatile Map<List<String>, Thread> runningBots = new HashMap<>();
	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service = Executors.newFixedThreadPool(5);

	public Set<List<String>> getAllBots() {
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getBlockchain().getObjectHeaders(OP_BOT, objectsSearchRequest);
		return objectsSearchRequest.resultWithHeaders;
	}

	public boolean startBot(String botName) {
		OpObject botObject = blocksManager.getBlockchain().getObjectByName(OP_BOT, botName);
		if (botObject == null) {
			return false;
		}

		BotPlacePublisher botPlacePublisher = new BotPlacePublisher(blocksManager, botObject, jdbcTemplate);
		futures.add(service.submit(botPlacePublisher));
		return true;
	}

	public boolean stopBot(String botName) {
//		OpObject botObject = blocksManager.getBlockchain().getObjectByName(OP_BOT, botName);
//		if (runningBots.containsKey(botObject.getId())) {
//			Thread thread = runningBots.get(botObject.getId());
//			thread.interrupt();
//			runningBots.remove(botObject.getId());
//			return true;
//		}

		return false;
	}

	// TODO add some info
	public Map<List<String>, String> getBotStats() {
//		Set<List<String>> bots = getAllBots();
//		HashMap<List<String>, String> botStats = new HashMap<>();
//		for (List<String> botId : bots) {
//			if (runningBots.containsKey(botId)) {
//				botStats.put(botId, "RUNNING");
//			} else {
//				botStats.put(botId, "NOT RUNNING");
//			}
//		}

		return Collections.EMPTY_MAP;
	}
}
