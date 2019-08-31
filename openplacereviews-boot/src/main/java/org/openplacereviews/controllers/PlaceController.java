package org.openplacereviews.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/api/places")
public class PlaceController {

	@Autowired
	public BlocksManager blocksManager;

	
	@GetMapping(path = "/geojson-ids")
	public ResponseEntity<String> serverLogin(HttpSession session) {
		OpBlockChain blc = blocksManager.getBlockchain();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
		r.requestOnlyKeys = true;
		blc.fetchAllObjects("opr.place", r);
		List<CompoundKey> ks = r.keys;
		Map<String, Integer> m = new HashMap<>();
		for(CompoundKey c : ks) {
			Integer l = m.get(c.first);
			if(l == null) {
				l = 1;
			} else {
				l = l + 1;
			}
			m.put(c.first, l);
		}
		return ResponseEntity.ok(m.toString());
	}
	
	@GetMapping(path = "/geojson-by-id")
	public ResponseEntity<String> serverLogin(HttpSession session, 
			@RequestParam(required = false) String tileId) {
		OpBlockChain blc = blocksManager.getBlockchain();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);
		
//		blc.fetchAllObjects("opr.place");
//		blocksManager.getBlockchain().getObjectByName("opr.place", o);	
		return ResponseEntity.ok(r.result.toString());
	}
}
