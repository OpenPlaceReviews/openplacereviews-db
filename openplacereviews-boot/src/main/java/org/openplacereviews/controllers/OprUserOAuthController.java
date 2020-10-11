package org.openplacereviews.controllers;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.openplacereviews.db.UserSchemaManager.OAuthUserDetails;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
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
import com.github.scribejava.apis.GitHubApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi10a;
import com.github.scribejava.core.builder.api.OAuth1SignatureType;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.github.scribejava.core.oauth.OAuthService;
import com.google.gson.reflect.TypeToken;

@Controller
@RequestMapping("/api/auth")
public class OprUserOAuthController {

	private static final String OSM_OAUTH_URL = "api/test-oauth.html";
	public static final String OAUTH_PROVIDER_OSM = "osm";
	public static final String OAUTH_PROVIDER_GITHUB = "github";
	public static final String OAUTH_PROVIDER_GOOGLE = "google";

	@Value("${oauth.osm.apiKey}")
	private String osmApiKey;

	@Value("${oauth.osm.secret}")
	private String osmApiSecret;
	
	@Value("${oauth.github.apiKey}")
	private String githubApiKey;

	@Value("${oauth.github.secret}")
	private String githubApiSecret;

	@Value("${opendb.serverUrl}")
	private String serverUrl;
	
	@Autowired
	private JsonFormatter formatter;

	private OAuth10aService osmService;
	private OAuth20Service githubService;

	private SelfExpiringHashMap<String, OAuth1RequestToken> requestTokens = 
			new SelfExpiringHashMap<>(TimeUnit.HOURS.toMillis(3));
	private SelfExpiringHashMap<String, OAuth1AccessToken> accessTokens = 
			new SelfExpiringHashMap<>(TimeUnit.HOURS.toMillis(3));

	private OAuth20Service getGithubService() {
		if (githubService != null) {
			return githubService;
		}
		githubService = new ServiceBuilder(githubApiKey).apiSecret(githubApiSecret).
				callback(serverUrl + OSM_OAUTH_URL).defaultScope("user:email").
				build(GitHubApi.instance());
		return githubService;
	}
	
	private OAuth10aService getOsmService() {
		if (osmService != null) {
			return osmService;
		}
		osmService = new ServiceBuilder(osmApiKey).apiSecret(osmApiSecret).callback(serverUrl + OSM_OAUTH_URL)
				.build(new DefaultApi10a() {

					@Override
					public String getAccessTokenEndpoint() {
						return "https://www.openstreetmap.org/oauth/access_token";
					}

					@Override
					protected String getAuthorizationBaseUrl() {
						return "https://www.openstreetmap.org/oauth/authorize";
					}
					
					@Override
					public OAuth1SignatureType getSignatureType() {
						return OAuth1SignatureType.QUERY_STRING;
					}

					@Override
					public String getRequestTokenEndpoint() {
						return "https://www.openstreetmap.org/oauth/request_token";
					}
					
				});
		return osmService;
	}
	
	@RequestMapping(path = "/user-oauth-auth")
	public String userOsmOauth(HttpServletResponse httpServletResponse, 
			@RequestParam(required = true) String oauthProvider)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException {
		OAuthService service = getOAuthProviderService(oauthProvider);
		String urlToAuthorize;
		if(service instanceof OAuth10aService) {
			OAuth1RequestToken token = ((OAuth10aService) service).getRequestToken();
			requestTokens.put(token.getToken(), token);
			urlToAuthorize = ((OAuth10aService) service).getAuthorizationUrl(token);
		} else if(service instanceof OAuth20Service) {
			urlToAuthorize = ((OAuth20Service) service).getAuthorizationUrl();
		} else {
			throw new UnsupportedOperationException("Unsupported oauth provider");
		}
//	    httpServletResponse.sendRedirect(u);
	    return "redirect:" + urlToAuthorize;
	}
	

	private OAuthService getOAuthProviderService(String oauthProvider) {
		if (oauthProvider.equalsIgnoreCase(OAUTH_PROVIDER_OSM)) {
			return getOsmService();
		}
		if (oauthProvider.equalsIgnoreCase(OAUTH_PROVIDER_GITHUB)) {
			return getGithubService();
		}
		throw new UnsupportedOperationException("Unsupported oauth provider");
	}

	@RequestMapping(path = "/user-oauth-postauth")
	@ResponseBody
	public ResponseEntity<String> userOsmOauth(HttpServletResponse httpServletResponse, 
			@RequestParam(required = false) String token, @RequestParam(required = false) String oauthVerifier
			, @RequestParam(required = false) String code)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException, XmlPullParserException {
		OAuthUserDetails userDetails;
		if(!OUtils.isEmpty(oauthVerifier)) {
			userDetails = authorizeOsmUserDetails(token, oauthVerifier);
		} else if(!OUtils.isEmpty(code)) {
			userDetails = authorizeGithub(code);
		} else {
			throw new IllegalArgumentException("Not enough parameters are specified");
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(userDetails));
	}
	

	
	private OAuthUserDetails authorizeGithub(String code) throws InterruptedException, ExecutionException, IOException {
		OAuthUserDetails userDetails = new OAuthUserDetails(OAUTH_PROVIDER_GITHUB);
		OAuth20Service githubService = getGithubService();
		OAuth2AccessToken accessToken = githubService.getAccessToken(code);
		userDetails.accessToken = accessToken.getAccessToken();
		// TODO
		// accessTokens.put(accessToken.getAccessToken(), accessToken);
		OAuthRequest req = new OAuthRequest(Verb.GET, "https://api.github.com/user");
		githubService.signRequest(userDetails.accessToken, req);
		req.addHeader("Content-Type", "application/json");
		Response response = githubService.execute(req);
		String body = response.getBody();
		TreeMap<String, Object> res = formatter.fromJsonToTreeMap(body);
		userDetails.uid = String.valueOf(res.get("id"));
		userDetails.nickname = String.valueOf(res.get("login"));
		Object aurl = res.get("avatar_url");
		if (aurl instanceof String) {
			userDetails.details.put(OAuthUserDetails.KEY_AVATAR_URL, (String) aurl);
		}
		OAuthRequest emlRequerst = new OAuthRequest(Verb.GET, "https://api.github.com/user/emails");
		githubService.signRequest(userDetails.accessToken, emlRequerst);
		emlRequerst.addHeader("Content-Type", "application/json");
		Response emailResponse = githubService.execute(emlRequerst);
		String emailBody = emailResponse.getBody();
		Type emailType = new TypeToken<List<TreeMap<String, Object>>>() {
		}.getType();
		List<TreeMap<String, Object>> lls = formatter.fromJson(new StringReader(emailBody), emailType);
		for (TreeMap<String, Object> eml : lls) {
			if (eml.get("email") instanceof String && Boolean.TRUE.equals(eml.get("verified"))) {
				userDetails.details.put(OAuthUserDetails.KEY_EMAIL, (String) eml.get("email"));
			}
		}
		return userDetails;
	}

	private OAuthUserDetails authorizeOsmUserDetails(String token, String oauthVerifier)
			throws InterruptedException, ExecutionException, IOException, XmlPullParserException {
		OAuthUserDetails details = new OAuthUserDetails(OAUTH_PROVIDER_OSM);
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
		List<String> languages = new ArrayList<>();
		int tok;
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if ("user".equals(name)) {
					details.nickname = parser.getAttributeValue("", "display_name");
					details.uid = parser.getAttributeValue("", "id");
				} else if ("home".equals(name)) {
					details.details.put(OAuthUserDetails.KEY_LAT, parser.getAttributeValue("", "lat"));
					details.details.put(OAuthUserDetails.KEY_LON, parser.getAttributeValue("", "lon"));
				} else if ("lang".equals(name)) {
					languages.add(parser.nextText());
				}
			}
		}
		details.details.put(OAuthUserDetails.KEY_LANGUAGES, formatter.fullObjectToJson(languages));
		return details;
	}

}