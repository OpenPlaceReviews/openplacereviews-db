package org.openplacereviews.db.controller;

import org.openplacereviews.db.service.OpTagsManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api")
public class TagController {

	@Autowired
	private OpTagsManager opTagsManager;

	@GetMapping(path = "/test")
	@ResponseBody
	public void check() {
		opTagsManager.mergeObjtablesAndTagMapping();
	}
}
