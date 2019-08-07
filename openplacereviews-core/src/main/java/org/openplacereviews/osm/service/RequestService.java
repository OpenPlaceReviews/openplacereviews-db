package org.openplacereviews.osm.service;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class RequestService {

	private final String overpassURL;
	private final String timestampURL;

	public RequestService(String overpassURL, String timestampURL) {
		this.overpassURL = overpassURL;
		this.timestampURL = timestampURL;
	}

	protected static final Log LOGGER = LogFactory.getLog(RequestService.class);

	public String getTimestamp() throws IOException {
		URL url = new URL(timestampURL);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		int responseCode = con.getResponseCode();
		LOGGER.debug("Sending 'GET' request to URL : " + url);
		LOGGER.debug("Response Code : " + responseCode);

		return IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
	}

	public Reader retrieveFile(String tags, String type, String timestamp) throws IOException {
		return retrieveFile(generateRequestString(tags, type, timestamp));
	}

	public Reader retrieveFile(String request) throws IOException {
		URL url = new URL(request);
		// TODO change to post request
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		//con.setRequestMethod("POST");
		LOGGER.debug("Sending 'POST' request to URL : " + url);
		LOGGER.debug("Response Code : " + con.getResponseCode());

		GZIPInputStream gzis = new GZIPInputStream(con.getInputStream());
		return new BufferedReader(new InputStreamReader(gzis));
	}

	public String generateRequestString(String tags, String type, String timestamp) throws UnsupportedEncodingException {
		String subRequest = "[out:xml][timeout:1200][%s:\"%s\"];(%s); out body; >; out geom meta;";
		String request = String.format(subRequest, type, timestamp, tags);

		request = URLEncoder.encode(request, StandardCharsets.UTF_8.toString());
		request = overpassURL+ "?data=" + request;

		return request;
	}
}
