package org.openplacereviews.db.service;

import org.openplacereviews.bot.BotPlacePublisher;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.openplacereviews.bot.BotPlacePublisher.OP_BOT;

@Service
public class BotManager {

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private volatile Map<List<String>, Thread> runningBots = new HashMap<>();

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

		if (!runningBots.containsKey(botObject.getId())) {
			BotPlacePublisher botPlacePublisher = new BotPlacePublisher(blocksManager, botObject, jdbcTemplate);
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					botPlacePublisher.publish();
					runningBots.remove(botObject.getId());
				}
			});
			runningBots.put(botObject.getId(), thread);
			thread.start();
			return true;
		}

		return false;
	}

	public boolean stopBot(String botName) {
		OpObject botObject = blocksManager.getBlockchain().getObjectByName(OP_BOT, botName);
		if (runningBots.containsKey(botObject.getId())) {
			Thread thread = runningBots.get(botObject.getId());
			thread.interrupt();
			runningBots.remove(botObject.getId());
			return true;
		}

		return false;
	}

	public Map<List<String>, String> getBotStats() {
		Set<List<String>> bots = getAllBots();
		HashMap<List<String>, String> botStats = new HashMap<>();
		for (List<String> botId : bots) {
			if (runningBots.containsKey(botId)) {
				botStats.put(botId, "RUNNING");
			} else {
				botStats.put(botId, "NOT RUNNING");
			}
		}

		return botStats;
	}
}
