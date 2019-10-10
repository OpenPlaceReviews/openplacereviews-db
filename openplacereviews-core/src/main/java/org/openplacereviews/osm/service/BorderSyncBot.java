package org.openplacereviews.osm.service;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

// FIXME
public class BorderSyncBot extends GenericMultiThreadBot<BorderSyncBot> {

	private int totalCnt = 1;
	private int progress = 0;

	public BorderSyncBot(OpObject botObject) {
		super(botObject, false);
		LOGGER = LogFactory.getLog(BorderSyncBot.class);
	}

	@Override
	public BorderSyncBot call() throws Exception {
		File directory = ResourceUtils.getFile("classpath:public/regions/");
		System.out.println(directory.getAbsolutePath());
		if (directory.exists()) {
			String path = "";
			Map<String, File> arr = new HashMap<>();
			for (File file1 : directory.listFiles()) {
				getRegions(file1, path, arr);
			}
			System.out.println(arr);
			// TODO generate borders area???
		} else {
			LOGGER.error("Error while generating borders");
		}
		return this;
	}

	private void getRegions(File file, String region, Map<String, File> list) {
		if (file.isDirectory()) {
			region += "/" + file.getName();
			for (File file1 : file.listFiles()) {
				getRegions(file1, region, list);
			}
		} else {
			region += "/" + FilenameUtils.getBaseName(file.getName());
			list.put(region, file.getAbsoluteFile());
		}
	}

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
		return "Generating borders";
	}

	@Override
	public String getTaskName() {
		return "border-sync";
	}
}
