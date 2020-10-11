package org.openplacereviews.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.github.pcan.SelfExpiringHashMap;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi10a;
import com.github.scribejava.core.builder.api.OAuth1SignatureType;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth10aService;

@Controller
@RequestMapping("/api/auth")
public class OsmOAuthController extends DefaultApi10a {

	private static final String OSM_OAUTH_URL = "api/test-osm-oauth.html";

	@Value("${oauth.osm.apiKey}")
	private String osmApiKey;

	@Value("${oauth.osm.secret}")
	private String osmApiSecret;

	@Value("${opendb.serverUrl}")
	private String serverUrl;
	
	@Autowired
	private JsonFormatter formatter;

	
	private OAuth10aService osmService;

	private SelfExpiringHashMap<String, OAuth1RequestToken> requestTokens = 
			new SelfExpiringHashMap<>(TimeUnit.HOURS.toMillis(3));
	private SelfExpiringHashMap<String, OAuth1AccessToken> accessTokens = 
			new SelfExpiringHashMap<>(TimeUnit.HOURS.toMillis(3));

	public static class OsmUserDetails {
		public String nickname;
		public List<String> languages = new ArrayList<String>();
		public double lat;
		public double lon;
		public long uid;

		public String accessToken;
	}

	@Override
	public OAuth1SignatureType getSignatureType() {
		return OAuth1SignatureType.QUERY_STRING;
	}

	@Override
	public String getRequestTokenEndpoint() {
		return "https://www.openstreetmap.org/oauth/request_token";
	}

	@Override
	public String getAccessTokenEndpoint() {
		return "https://www.openstreetmap.org/oauth/access_token";
	}

	@Override
	protected String getAuthorizationBaseUrl() {
		return "https://www.openstreetmap.org/oauth/authorize";
	}
	
	private OAuth10aService getOsmService() {
		if (osmService != null) {
			return osmService;
		}
		osmService = new ServiceBuilder(osmApiKey).apiSecret(osmApiSecret).callback(serverUrl + OSM_OAUTH_URL)
				.build(this);
		return osmService;
	}
	
	@RequestMapping(path = "/user-osm-auth")
	public String userOsmOauth(HttpServletResponse httpServletResponse)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException {
		OAuth10aService service = getOsmService();
		OAuth1RequestToken token = service.getRequestToken();
		requestTokens.put(token.getToken(), token);
		String urlToAuthorize = service.getAuthorizationUrl(token);
//	    httpServletResponse.sendRedirect(u);
	    return "redirect:" + urlToAuthorize;
	}
	
	@RequestMapping(path = "/user-osm-postauth")
	@ResponseBody
	public ResponseEntity<String> userOsmOauth(HttpServletResponse httpServletResponse, 
			@RequestParam(required = true) String token, @RequestParam(required = true) String oauthVerifier)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException, XmlPullParserException {
		OsmUserDetails osmUserDetails = authorizeOsmUserDetails(token, oauthVerifier);
		return ResponseEntity.ok(formatter.fullObjectToJson(osmUserDetails));
	}

	
	public OsmUserDetails authorizeOsmUserDetails(String token, String oauthVerifier)
			throws InterruptedException, ExecutionException, IOException, XmlPullParserException {
		OsmUserDetails details = new OsmUserDetails();
		OAuth1RequestToken requestToken = requestTokens.remove(token);
		OAuth1AccessToken accessToken = null;
		if (requestToken == null) {
			accessToken = accessTokens.get(token);
		}
		if (requestToken == null && accessToken == null) {
			throw new IllegalArgumentException("Illegal request token: " + token);
		}
		if (accessToken == null) {
			accessToken = osmService.getAccessToken(requestToken, oauthVerifier);
			
		}
		details.accessToken = accessToken.getToken(); 
		accessTokens.put(accessToken.getToken(), accessToken);
		// save for reuse of request ( they usually don't match and should'nt be an issue)
		accessTokens.put(token, accessToken);
		String url = "https://api.openstreetmap.org/api/0.6/user/details";
		OAuthRequest req = new OAuthRequest(Verb.GET, url);
		osmService.signRequest(accessToken, req);
		req.addHeader("Content-Type", "application/xml");
		Response response = osmService.execute(req);
		XmlPullParser parser = new org.kxml2.io.KXmlParser();
		parser.setInput(response.getStream(), "UTF-8");
		int tok;
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if ("user".equals(name)) {
					details.nickname = parser.getAttributeValue("", "display_name");
					details.uid = Long.parseLong(parser.getAttributeValue("", "id"));
				} else if ("home".equals(name)) {
					details.lat = Double.parseDouble(parser.getAttributeValue("", "lat"));
					details.lon = Double.parseDouble(parser.getAttributeValue("", "lon"));
				} else if ("lang".equals(name)) {
					details.languages.add(parser.nextText());
				}
			}
		}
		return details;
	}

}