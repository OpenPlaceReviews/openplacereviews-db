package org.openplacereviews.osm.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OprUtil {

	private static final Log LOGGER = LogFactory.getLog(OprUtil.class);

	// utility methods
	public static HttpURLConnection connect(String urlReq, String msg) throws MalformedURLException, IOException {
		long tm = System.currentTimeMillis();
		URL url = new URL(urlReq);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		int cd = con.getResponseCode();
		String rmsg = con.getResponseMessage();
		tm = System.currentTimeMillis() - tm;
		// LOGGER.info(String.format("%s: %s - %d ms, %d %s", msg, urlReq, tm, cd, rmsg));
		LOGGER.info(String.format("%s: %s - %d ms, %d %s", msg, url.getHost(), tm, cd, rmsg));
		return con;
	}
	
	public static String downloadString(String urlReq, String msg) throws IOException {
		HttpURLConnection con = connect(urlReq, msg);
		String res = IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
		con.disconnect();
		return res;
	}

	public static BufferedReader downloadGzipReader(String request, String msg) throws IOException {
		HttpURLConnection con = connect(request, msg);
		GZIPInputStream gzis = new GZIPInputStream(con.getInputStream());
		return new BufferedReader(new InputStreamReader(gzis));
	}

}
