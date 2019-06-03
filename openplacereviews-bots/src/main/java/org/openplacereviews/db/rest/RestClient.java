package org.openplacereviews.db.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.dto.OsmCoordinatePlacesDto;
import org.openplacereviews.db.exception.BadCredentialsException;
import org.openplacereviews.db.publisher.OsmCoordinatePlacePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

import static org.springframework.http.HttpHeaders.SET_COOKIE;

@Service
public class RestClient {
	protected static final Log LOGGER = LogFactory.getLog(OsmCoordinatePlacePublisher.class);

	private final static String LOGIN_FORM = "name";

	private final static String PASSWORD_FORM = "pwd";

	@Autowired
	private RestTemplate restTemplate;

	@Value("${url.api}")
	private String openDbUrl;

	@Value("${admin.login}")
	private String login;

	@Value("${admin.password}")
	private String password;

	@Value("${cookie.expiration.time}")
	private long cookieExpirationTime;

	@Autowired
	private ObjectMapper mapper;

	private String loginUrl;

	private String processOpUrl;

	private String createBlockUri;

	private String authCookie;

	private long cookieUpdatedTime;

	@PostConstruct
	private void postConstructor() {
		this.loginUrl = openDbUrl + "/auth/admin-login";
		this.processOpUrl = openDbUrl + "/auth/process-operation";
		this.createBlockUri = openDbUrl + "/mgmt/create";
	}

	protected void login() {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

		MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
		map.add(LOGIN_FORM, login);
		map.add(PASSWORD_FORM, password);

		HttpEntity<MultiValueMap<String, String>> authRequest = new HttpEntity<>(map, headers);

		ResponseEntity<String> responseEntity = restTemplate.postForEntity(loginUrl, authRequest, String.class);
		if (responseEntity.getStatusCode() != HttpStatus.OK) {
			throw new BadCredentialsException();
		}

		HttpHeaders httpHeaders = responseEntity.getHeaders();
		List<String> cookie = httpHeaders.get(SET_COOKIE);
		if (cookie == null || cookie.isEmpty()) {
			throw new IllegalStateException("App hasn't received cookie from auth request.");
		}

		authCookie = cookie.get(0);
		cookieUpdatedTime = System.currentTimeMillis();
	}

	public void postOsmCoordinatePlace(OsmCoordinatePlacesDto osmCoordinatePlaceDto) throws IOException {
		if (authCookie == null)
			login();

		HttpHeaders headers = new HttpHeaders();
		headers.set(HttpHeaders.COOKIE, authCookie);
		headers.setContentType(MediaType.APPLICATION_JSON);

		String body = mapper.writeValueAsString(osmCoordinatePlaceDto);

		HttpEntity addOsmOpRequest = new HttpEntity(body, headers);

		ResponseEntity<String> responseEntity =
				restTemplate.postForEntity(processOpUrl + "?addToQueue=true", addOsmOpRequest, String.class);
		if (responseEntity.getStatusCode() != HttpStatus.OK) {
			LOGGER.error("Error occurred while posting place: " + mapper.writeValueAsString(osmCoordinatePlaceDto));
		}
	}

	public void createBlock() {
		updatedCookieIfNeed();
		HttpHeaders headers = new HttpHeaders();
		headers.set(HttpHeaders.COOKIE, authCookie);
		headers.setContentType(MediaType.APPLICATION_JSON);

		HttpEntity addOsmOpRequest1 = new HttpEntity(null, headers);
		ResponseEntity<String> responseEntity1 =
				restTemplate.postForEntity(createBlockUri, addOsmOpRequest1, String.class);
		LOGGER.info("Block created: " + responseEntity1.getBody());
	}

	/**
	 * Updated login cookie. If authCookie is null or when it expired.
	 */
	private void updatedCookieIfNeed() {
		if (authCookie == null) {
			login();
		} else {
			long currentTime = System.currentTimeMillis();
			if (currentTime - cookieUpdatedTime > cookieExpirationTime) {
				login();
			}
		}
	}

}
