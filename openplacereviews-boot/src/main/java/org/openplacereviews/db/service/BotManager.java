package org.openplacereviews.db.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.osm.service.IBotManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.osm.service.PublishBotManager.OP_BOT;

@Service
public class BotManager {

	private static final Log LOGGER = LogFactory.getLog(BotManager.class);

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service = Executors.newFixedThreadPool(5);

	public Set<List<String>> getAllBots() {
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		objectsSearchRequest.field = F_ID;
		blocksManager.getBlockchain().getObjectHeaders(OP_BOT, objectsSearchRequest);
		return objectsSearchRequest.resultWithHeaders;
	}

	public boolean startBot(String botName) {
		OpObject botObject = blocksManager.getBlockchain().getObjectByName(OP_BOT, botName);
		if (botObject == null) {
			return false;
		}

		IBotManager botInterface = null;
		try {
			Class<?> bot = Class.forName(botObject.getStringObjMap("bot").get("API").toString());
			Constructor constructor = bot.getConstructor(BlocksManager.class, OpObject.class, JdbcTemplate.class);
			botInterface = (IBotManager) constructor.newInstance(blocksManager, botObject, jdbcTemplate);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			LOGGER.error("Error while creating bot instance", e);
		}

		if (botInterface != null) {
			futures.add(service.submit(botInterface));
			return true;
		}

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
