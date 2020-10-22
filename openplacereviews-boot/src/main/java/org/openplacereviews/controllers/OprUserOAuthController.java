package org.openplacereviews.controllers;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpSession;

import org.openplacereviews.db.UserSchemaManager;
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

import com.github.scribejava.apis.GitHubApi;
import com.github.scribejava.apis.GoogleApi20;
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

	public static final String OAUTH_PROVIDER_OSM = "osm";
	public static final String OAUTH_PROVIDER_GITHUB = "github";
	public static final String OAUTH_PROVIDER_GOOGLE = "google";
	
	private static final String REQUEST_TOKEN = "request_token";
	private static final String USER_DETAILS = "user_details";
	private static final String OAUTH_PROVIDER = "oauth_provider";

	@Value("${oauth.osm.apiKey}")
	private String osmApiKey;

	@Value("${oauth.osm.secret}")
	private String osmApiSecret;
	
	@Value("${oauth.github.apiKey}")
	private String githubApiKey;

	@Value("${oauth.github.secret}")
	private String githubApiSecret;
	
	@Value("${oauth.google.apiKey}")
	private String googleApiKey;

	@Value("${oauth.google.secret}")
	private String googleApiSecret;

	@Value("${opendb.serverUrl}")
	private String serverUrl;
	
	@Value("${opendb.authUrl}")
	private String authUrl;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	public UserSchemaManager userSchemaManager;

	private OAuth10aService osmService;
	private OAuth20Service githubService;
	private OAuth20Service googleService;

	private OAuth20Service getGithubService() {
		if (githubService != null) {
			return githubService;
		}
		githubService = new ServiceBuilder(githubApiKey).apiSecret(githubApiSecret).
				callback(serverUrl + authUrl).defaultScope("user:email").
				build(GitHubApi.instance());
		return githubService;
	}
	
	private OAuth20Service getGoogleService() {
		if (googleService != null) {
			return googleService;
		}
		// https://www.googleapis.com/auth/userinfo.email
		googleService = new ServiceBuilder(googleApiKey).apiSecret(googleApiSecret).
				defaultScope("https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email").callback(serverUrl + authUrl).
				build(GoogleApi20.instance());
		return googleService;
	}
	
	private OAuth10aService getOsmService() {
		if (osmService != null) {
			return osmService;
		}
		osmService = new ServiceBuilder(osmApiKey).apiSecret(osmApiSecret).callback(serverUrl + authUrl)
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
	public String userOsmOauth(HttpSession httpSession,
			@RequestParam(required = true) String oauthProvider)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException {
		OAuthService service = getOAuthProviderService(oauthProvider);
		// logout from session
		httpSession.removeAttribute(USER_DETAILS);
		httpSession.setAttribute(OAUTH_PROVIDER, oauthProvider);
		String urlToAuthorize;
		if(service instanceof OAuth10aService) {
			OAuth1RequestToken token = ((OAuth10aService) service).getRequestToken();
			httpSession.setAttribute(REQUEST_TOKEN, token);
			urlToAuthorize = ((OAuth10aService) service).getAuthorizationUrl(token);
		} else if(service instanceof OAuth20Service) {
			urlToAuthorize = ((OAuth20Service) service).getAuthorizationUrl();
		} else {
			throw new UnsupportedOperationException("Unsupported oauth provider");
		}
	    return "redirect:" + urlToAuthorize;
	}
	

	private OAuthService getOAuthProviderService(String oauthProvider) {
		if (oauthProvider.equalsIgnoreCase(OAUTH_PROVIDER_OSM)) {
			return getOsmService();
		} else if (oauthProvider.equalsIgnoreCase(OAUTH_PROVIDER_GITHUB)) {
			return getGithubService();
		} else if (oauthProvider.equalsIgnoreCase(OAUTH_PROVIDER_GOOGLE)) {
			return getGoogleService();
		}
		throw new UnsupportedOperationException("Unsupported oauth provider");
	}

	@RequestMapping(path = "/user-oauth-postauth")
	@ResponseBody
	public ResponseEntity<String> userPostOauth(HttpSession httpSession, 
			@RequestParam(required = false) String token, @RequestParam(required = false) String oauthVerifier,
			@RequestParam(required = false) String code)
			throws FailedVerificationException, IOException, InterruptedException, ExecutionException, XmlPullParserException {
		OAuthUserDetails userDetails = (OAuthUserDetails) httpSession.getAttribute(USER_DETAILS);
		String provider = (String) httpSession.getAttribute(OAUTH_PROVIDER);
		if (userDetails != null) {
			// details are present
			if(!OUtils.equalsStringValue(userDetails.requestUserCode, code) && !OUtils.equalsStringValue(userDetails.requestUserCode, oauthVerifier)) {
				throw new IllegalArgumentException("XSS / CSRF protection: please submit oauth request code to get details");
			}
		} else {
			if (OAUTH_PROVIDER_OSM.equals(provider)) {
				if (!OUtils.isEmpty(oauthVerifier)) {
					userDetails = authorizeOsmUserDetails(httpSession, token, oauthVerifier);
					userDetails.requestUserCode = oauthVerifier;
				}
			} else if (OAUTH_PROVIDER_GITHUB.equals(provider)) {
				if (!OUtils.isEmpty(code)) {
					userDetails = authorizeGithub(httpSession, code);
					userDetails.requestUserCode = code;
				}
			} else if (OAUTH_PROVIDER_GOOGLE.equals(provider)) {
				if (!OUtils.isEmpty(code)) {
					userDetails = authorizeGoogle(httpSession, code);
					userDetails.requestUserCode = code;
				}
			}
			userSchemaManager.loadPossibleSignups(userDetails);
		}
		if (userDetails == null) {
			throw new IllegalArgumentException("Not enough parameters are specified");
		}
		httpSession.setAttribute(USER_DETAILS, userDetails);
		return ResponseEntity.ok(formatter.fullObjectToJson(userDetails));
	}

	
	

	public OAuthUserDetails getUserDetails(HttpSession httpSession) {
		return (OAuthUserDetails) httpSession.getAttribute(USER_DETAILS);
	}
	
	private OAuthUserDetails authorizeGoogle(HttpSession httpSession, String code) throws InterruptedException, ExecutionException, IOException {
		OAuthUserDetails userDetails = new OAuthUserDetails(OAUTH_PROVIDER_GOOGLE);
		OAuth20Service googleService = getGoogleService();
		OAuth2AccessToken accessToken = googleService.getAccessToken(code);
		userDetails.accessToken = UUID.randomUUID().toString();
		userDetails.accessTokenSecret = accessToken.getAccessToken();
		
		Map<String, Object> res = getJsonInfoMap(userDetails, googleService, "https://www.googleapis.com/oauth2/v1/userinfo?alt=json");
		userDetails.oauthUid = String.valueOf(res.get("id"));
		userDetails.oauthNickname = String.valueOf(res.get("name"));
		userDetails.details.put(OAuthUserDetails.KEY_NICKNAME, userDetails.oauthNickname);
		Object aurl = res.get("picture");
		if (aurl instanceof String) {
			userDetails.details.put(OAuthUserDetails.KEY_AVATAR_URL, (String) aurl);
		}
		if (res.get("locale") != null) {
			// "verified_email": true,
			userDetails.details.put(OAuthUserDetails.KEY_LANGUAGES, (String) res.get("locale"));
		}
		if (res.get("email") != null) {
			// "verified_email": true,
			userDetails.details.put(OAuthUserDetails.KEY_EMAIL, (String) res.get("email"));
		}
		return userDetails;
	}

	private Map<String, Object> getJsonInfoMap(OAuthUserDetails userDetails, OAuth20Service githubService, String url)
			throws InterruptedException, ExecutionException, IOException {
		OAuthRequest req = new OAuthRequest(Verb.GET, url);
		githubService.signRequest(userDetails.accessTokenSecret, req);
		req.addHeader("Content-Type", "application/json");
		Response response = githubService.execute(req);
		String body = response.getBody();
		return formatter.fromJsonToTreeMap(body);
	}
	
	private OAuthUserDetails authorizeGithub(HttpSession httpSession, String code) throws InterruptedException, ExecutionException, IOException {
		OAuthUserDetails userDetails = new OAuthUserDetails(OAUTH_PROVIDER_GITHUB);
		OAuth20Service githubService = getGithubService();
		OAuth2AccessToken accessToken = githubService.getAccessToken(code);
		userDetails.accessToken = UUID.randomUUID().toString();
		userDetails.accessTokenSecret = accessToken.getAccessToken();
		Map<String, Object> res = getJsonInfoMap(userDetails, githubService, "https://api.github.com/user");
		userDetails.oauthUid = String.valueOf(res.get("id"));
		userDetails.oauthNickname = String.valueOf(res.get("login"));
		userDetails.details.put(OAuthUserDetails.KEY_NICKNAME, userDetails.oauthNickname);
		Object aurl = res.get("avatar_url");
		if (aurl instanceof String) {
			userDetails.details.put(OAuthUserDetails.KEY_AVATAR_URL, (String) aurl);
		}
		OAuthRequest emlRequerst = new OAuthRequest(Verb.GET, "https://api.github.com/user/emails");
		githubService.signRequest(userDetails.accessTokenSecret, emlRequerst);
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
	

	private OAuthUserDetails authorizeOsmUserDetails(HttpSession httpSession, String token, String oauthVerifier)
			throws InterruptedException, ExecutionException, IOException, XmlPullParserException {
		OAuthUserDetails details = new OAuthUserDetails(OAUTH_PROVIDER_OSM);
		OAuth1RequestToken requestToken = (OAuth1RequestToken) httpSession.getAttribute(REQUEST_TOKEN);
		if (requestToken == null) {
			throw new IllegalArgumentException("Illegal request token: " + token);
		}
		OAuth10aService osmService = getOsmService();
		OAuth1AccessToken accessToken = osmService.getAccessToken(requestToken, oauthVerifier);
		details.accessToken = accessToken.getToken(); 
		details.accessTokenSecret = accessToken.getTokenSecret();
		// save for reuse of request ( they usually don't match and should'nt be an issue)
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
					details.oauthNickname = parser.getAttributeValue("", "display_name");
					details.details.put(OAuthUserDetails.KEY_NICKNAME, details.oauthNickname);
					details.oauthUid = parser.getAttributeValue("", "id");
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