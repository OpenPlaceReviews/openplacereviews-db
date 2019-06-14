package org.openplacereviews.db.config;

import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class OsmConfiguration {

	@Autowired
	public BlocksManager blocksManager;

	@PostConstruct
	public void postConstructor() {
		//Added osm operation to bootstrap list
		blocksManager.addToBootstrap("opr-osm");
	}
}
