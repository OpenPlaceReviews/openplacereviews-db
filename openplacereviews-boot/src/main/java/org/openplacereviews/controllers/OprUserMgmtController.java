package org.openplacereviews.controllers;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.UserSchemaManager;
import org.openplacereviews.db.UserSchemaManager.OAuthUserDetails;
import org.openplacereviews.db.UserSchemaManager.UserStatus;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.OpOperation.OpObjectDiffBuilder;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.sendgrid.Content;
import com.sendgrid.Email;
import com.sendgrid.Mail;
import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;

@Controller
@RequestMapping("/api/auth")
public class OprUserMgmtController {
	
	// IN FUTURE: how to change signup methods (pwd <-> OAUTH)
	// IN FUTURE: ask user-Email at login if it's missing in DB   
	
	// CSRF / XSS
	// Could be configured in WebSecurityConfiguration class 
	// - Login provides privateKey which could be stored for now in a cookie
	// - There is no way to use Session or Cookie to authorize operations for now
	// - Each time private key should be passed as a form parameter and taken from cookie or internal 
	// - We don't use CrossOrigin / CORS restrictions cause there are multiple clients Android, Web
	// - Web-client always need to check if cookie expired or not 
	// In future:
	// - We can provide special token instead of private specially for Web-Client and implement CORS
	// - We can also implement CORS (always check Origin:HOST for <username>:opr-web login and don't use it for <username>:osmand)

	
	// Short introduction of user management:
	// - BLOCKCHAIN: Signup / login objects are maintained by blockchain and could be not synchronized with database.
	// - BLOCKCHAIN doesn't have any private key and doesn't have email (or any other personal info)
	// - DB: user_table has user name, signup private key (if present - not oauth), login private key (so user can do operations in blockchain).
	// - CLIENT: clients receives through API *login* private key after email is confirmed
	// - CLIENT: could revoke login private key (logout), get new private key (login) or change signup private key (change password)
	
	// THESE ENTITIES should be synchronized, so unexpected scenarios should be handled:
	// LOGIN: CLIENT private key <-> matches DB login private key <-> matches Blockchain public key (security check)
	// SIGNUP: DB signup private key <-> matches signup Blockchain public key (security check)
	
	// Unexpected scenarios:
	// 0. Client login private key doesn't match db private key:
	//    - Test: with api method user-check-loginkey
	//    - Solution: logout / login (doesn't require loginkey)
	// 1. User login was deleted or has changed in blockchain (signup is still present): UI -> 
	//    - Test: with api method user-check-loginkey
	//    - Solution: logout / login (doesn't require loginkey)
	// 2. User signup is present in blockchain but it is not in database:
    //    - Solution: ui can check /user-status and suggest login
	//    - User will select Login anyway which should work fine
	//    - IN FUTURE: we could ask email / confirm it if user email wasn't registered
	// 3. Sign up Email token expired 24 H
	//    - Solution: suggest user to signup again
	//    - There is no check if user is already in database during signup (there is only check if user is already in blockchain)
	//    - So Multiple signup is OK before email confirmation & blockchain registration.
	// 4. Email to reset password has expired
	//    - Solution: reset password again
	// 5. User signup has changed in blockchain or deleted but the old one is present in database :
	//    - This could also happen if email was sent and user signed up in between 2 emails
	//    - Solution: User can login again and signup key will be updated
	//    - Client could check that situation happened by cal /user-check-signupkey
	
	// In future: allow to change oauth <-> pwd
	// In future: user can specify private key (token) for web operations
	
	protected static final Log LOGGER = LogFactory.getLog(OprUserMgmtController.class);
	
	// TODO make url customizable
	public static final String DEFAULT_PURPOSE_LOGIN = "opr-web";
	
	@Value("${opendb.email.sendgrid-api}")
	private String sendGridApiKey;
	
	@Value("${opendb.authUrl}")
	private String authUrl;

	@Value("${opendb.serverUrl}")
	private String serverUrl;

	@Autowired
	private BlocksManager manager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private UserSchemaManager userManager;
	
	@Autowired
	private OprUserOAuthController userOAuthController;

	private SendGrid sendGridClient;

	@RequestMapping(path = "/user-signup-confirm")
	@ResponseBody
	public ResponseEntity<String> signupConfirm(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = true) String token, 
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose,
			@RequestParam(required = false) String userDetails) throws FailedVerificationException {
		name = stdNickName(name);
		OpOperation signupOp = userManager.validateEmail(name, token);
		OpObject signupObj = manager.getLoginObj(name);
		boolean userAlreadySignedUp = false;
		if(signupObj != null) {
			 userAlreadySignedUp = true;
		} else if(signupOp != null) {
			signupObj = signupOp.getCreated().get(0);
		} else {
			throw new IllegalStateException("User signup operation wasn't successful");
		}
		String signupMethod = signupObj.getStringValue(OpBlockchainRules.F_AUTH_METHOD);
		KeyPair ownKeyPair = null;
		if (OpBlockchainRules.METHOD_OAUTH.equals(signupMethod)) {
			OAuthUserDetails oAuthUserDetails = userManager.getOAuthLatestLogin(name);
			validateOAuthMethod(oAuthUserDetails, signupObj);
			// if it's operation to consolidate signup we don't need to add operation to blockchain
			if (!userAlreadySignedUp) {
				signupOp.setSignedBy(manager.getServerUser());
				manager.generateHashAndSign(signupOp, manager.getServerLoginKeyPair());
				// 1. Add operation, so the user signup is confirmed and everything is ok
				// In case user already exists, operation will fail and user will need to login or wait email expiration
				manager.addOperation(signupOp);
			}
		} else if (OpBlockchainRules.METHOD_PWD.equals(signupMethod)) {
			String sPrivKey = userManager.getSignupPrivateKey(name);
			String sPubKey = signupObj.getStringValue(OpBlockchainRules.F_PUBKEY);
			String algo = signupObj.getStringValue(OpBlockchainRules.F_ALGO);
			ownKeyPair = SecUtils.getKeyPair(algo, sPrivKey, sPubKey);
			if (!SecUtils.validateKeyPair(algo, ownKeyPair.getPrivate(), ownKeyPair.getPublic())) {
				throw new IllegalStateException(
						"Signup operation is not successful, please try to login with different password.");
			}
			// if it's operation to consolidate signup we don't need to add operation to blockchain
			if (!userAlreadySignedUp) {
				signupOp.setSignedBy(name);
				signupOp.addOtherSignedBy(manager.getServerUser());
				manager.generateHashAndSign(signupOp, ownKeyPair, manager.getServerLoginKeyPair());
				// 1. Add operation, so the user signup is confirmed and everything is ok
				// In case user already exists, operation will fail and user will need to login or wait email expiration
				manager.addOperation(signupOp);
			}
		} else {
			throw new IllegalArgumentException("Unknown signup method was used: " + signupMethod);
		}
		// 2. Signup was added, so login will be generated 
		return generateNewLogin(name, ownKeyPair, userDetails, purpose);
	}


	
	
	
	@GetMapping(path = "/user-check-signupkey")
	@ResponseBody
	public ResponseEntity<String> checkLogin(HttpSession session, @RequestParam(required = true) String name) throws FailedVerificationException {
		name = stdNickName(name);
		OpObject loginObj = manager.getLoginObj(name );
		if (loginObj == null) {
			throw new IllegalStateException("User is not logged in into blockchain");
		}
		String signupPrivateKey = userManager.getSignupPrivateKey(name);
		if (signupPrivateKey == null) {
			throw new IllegalStateException("User is not logged in to the website");
		}
		String algo = loginObj.getStringValue(OpBlockchainRules.F_ALGO);
		KeyPair signKeyPair = SecUtils.getKeyPair(algo, signupPrivateKey, loginObj.getStringValue(OpBlockchainRules.F_PUBKEY));
		if(!SecUtils.validateKeyPair(algo, signKeyPair.getPrivate(), signKeyPair.getPublic())) {
			throw new IllegalStateException("User db private key doesn't have public key in blockchain");
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(Collections.singletonMap("result", "OK")));
	}

	@GetMapping(path = "/user-check-loginkey")
	@ResponseBody
	public ResponseEntity<String> checkLogin(HttpSession session, @RequestParam(required = true) String name, 
			@RequestParam(required = true) String privateKey,
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose) throws FailedVerificationException {
		name = stdNickName(name);
		OpObject loginObj = manager.getLoginObj(name + ":" + purpose);
		if (loginObj == null) {
			throw new IllegalStateException("User is not logged in into blockchain");
		}
		if (OprUserMgmtController.DEFAULT_PURPOSE_LOGIN.equals(purpose)) {
			String loginPrivateKey = userManager.getLoginPrivateKey(name, purpose);
			if (loginPrivateKey == null) {
				throw new IllegalStateException("User is not logged in to the website");
			}
			if (!OUtils.equalsStringValue(loginPrivateKey, privateKey)) {
				throw new IllegalStateException("Private key provided by the client doesn't match db key");
			}
		}
		String algo = loginObj.getStringValue(OpBlockchainRules.F_ALGO);
		KeyPair signKeyPair = SecUtils.getKeyPair(algo, 
				privateKey, loginObj.getStringValue(OpBlockchainRules.F_PUBKEY));
		if(!SecUtils.validateKeyPair(algo, signKeyPair.getPrivate(), signKeyPair.getPublic())) {
			throw new IllegalStateException("User db private key doesn't have public key in blockchain");
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(Collections.singletonMap("result", "OK")));
	}

	@GetMapping(path = "/user-status")
	@ResponseBody
	public ResponseEntity<String> userExists(@RequestParam(required = true) String name)
			throws FailedVerificationException {
		name = stdNickName(name);
		OpObject signupObj = manager.getLoginObj(name);
		Map<String, String> mp = new TreeMap<String, String>();
		mp.put("blockchain", signupObj == null ? "none" : "ok");
		UserStatus status = userManager.userGetStatus(name);
		mp.put("db-name", status == null ? "none" : "ok");
		if (status != null) {
			String signupPrivateKey = userManager.getSignupPrivateKey(name);
			mp.put("db-key", signupPrivateKey == null ? "none" : "ok");
			mp.put("email", OUtils.isEmpty(status.email) ? "none" : "ok");
			mp.put("email-expired", Boolean.toString(status.tokenExpired));
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(mp));
	}

	
	
	@PostMapping(path = "/user-reset-password-email")
	@ResponseBody
	public ResponseEntity<String> resetPwdSendEmail(HttpSession session, @RequestParam(required = true) String name, 
			@RequestParam(required = true) String email,
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose) throws FailedVerificationException {
		name = stdNickName(name);
		checkUserSignupPrivateKeyIsPresent(name);
		UserStatus status = userManager.userGetStatus(name);
		String userEmail =  status == null ? null : status.email;
		if (OUtils.isEmpty(userEmail)) {
			throw new IllegalStateException("User email was not registered");
		}
		if (!OUtils.equals(userEmail, email)) {
			throw new IllegalStateException("Provided email doesn't match email in the database");
		}
		deleteLoginIfPresent(name, purpose);
		String emailToken = generateEmailToken();
		String href = getServerUrl() + authUrl + "?op=reset_pwd&name=" + name + "&token=" + emailToken;
		sendEmail(name, email, href, "OpenPlaceReviews - Reset password", getResetEmailContent(name, href, emailToken).toString());
		userManager.resetEmailToken(name, emailToken);
		return ResponseEntity.ok(formatter.fullObjectToJson(Collections.singletonMap("result", "OK")));
	}

	private static String generateEmailToken() {
		return UUID.randomUUID().toString().substring(0, 8);
	}
	
	@PostMapping(path = "/user-reset-password-confirm")
	@ResponseBody
	public ResponseEntity<String> resetPwdConfirm(HttpSession session, @RequestParam(required = true) String name, 
			@RequestParam(required = true) String token, 
			@RequestParam(required = true) String newPwd, 
			@RequestParam(required = false) String userDetails, 
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose) throws FailedVerificationException {
		name = stdNickName(name);
		checkUserSignupPrivateKeyIsPresent(name);
		userManager.validateEmail(name, token);
		OpObject signupObj = manager.getLoginObj(name);
		if (signupObj == null) {
			throw new IllegalStateException("User was not signed up");
		}
		// could be same for private key as well
		if (!OpBlockchainRules.METHOD_PWD.equals(signupObj.getStringValue(OpBlockchainRules.F_AUTH_METHOD))) {
			throw new IllegalStateException("Wrong signup method was used: " + signupObj.getStringValue(OpBlockchainRules.F_AUTH_METHOD));
		}
		KeyPair oldSignKeyPair = SecUtils.getKeyPair(signupObj.getStringValue(OpBlockchainRules.F_ALGO), 
				userManager.getSignupPrivateKey(name), signupObj.getStringValue(OpBlockchainRules.F_PUBKEY));
		
		String algo = SecUtils.ALGO_EC;
		String salt = name;
		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
		KeyPair newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, newPwd, true);
		
		OpObjectDiffBuilder bld = OpOperation.createDiffOperation(signupObj);
		bld.setNewTag(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPublic()));
		bld.setNewTag(OpBlockchainRules.F_SALT, salt);
		bld.setNewTag(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
		bld.setNewTag(OpBlockchainRules.F_ALGO, algo);

		OpOperation editSigninOp = bld.build();
		editSigninOp.setSignedBy(name);
		editSigninOp.addOtherSignedBy(manager.getServerUser());
		manager.generateHashAndSign(editSigninOp, oldSignKeyPair, manager.getServerLoginKeyPair());
		manager.addOperation(editSigninOp);

		String privateKey = SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPrivate());
		if (userManager.userGetStatus(name) == null) {
			userManager.createNewUser(name, null, null, null, privateKey, null);
		} else if (!OUtils.equalsStringValue(userManager.getSignupPrivateKey(name), privateKey)) {
			userManager.updateSignupKey(name, privateKey);
		}
		return generateNewLogin(name, newKeyPair, userDetails, purpose);
	}

	private void checkUserSignupPrivateKeyIsPresent(String name) {
		String spk = userManager.getSignupPrivateKey(name);
		if (spk == null || spk.length() == 0) {
			throw new IllegalStateException("User was not registered using any password");
		}
	}
	
	@PostMapping(path = "/user-login")
	@ResponseBody
	public ResponseEntity<String> login(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = false) String pwd, 
			@RequestParam(required = false) String oauthAccessToken,
			@RequestParam(required = false) String email, @RequestParam(required = false) String userDetails, 
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose) throws FailedVerificationException {
		name = stdNickName(name);
		OpObject signupObj = manager.getLoginObj(name);
		if (signupObj == null) {
			throw new IllegalStateException("User was not signed up in blockchain");
		}
		String signupMethod = signupObj.getStringValue(OpBlockchainRules.F_AUTH_METHOD);
		KeyPair ownKeyPair = null;
		if (OpBlockchainRules.METHOD_OAUTH.equals(signupMethod)) {
			OAuthUserDetails oAuthUserDetails = userOAuthController.getUserDetails(session);
			validateOAuthMethod(oAuthUserDetails, signupObj);
			// possible csrf attack check 
			if(!oAuthUserDetails.accessToken.equals(oauthAccessToken)) {
				throw new IllegalArgumentException("User wasn't registered with OAuth on this website.");
			}
		} else if (OpBlockchainRules.METHOD_PWD.equals(signupMethod)) {
			ownKeyPair = validateLoginPwd(pwd, signupObj);
			String privateKey = SecUtils.encodeKey(SecUtils.KEY_BASE64, ownKeyPair.getPrivate());
			if (userManager.userGetStatus(name) == null) {
				userManager.createNewUser(name, null, null, null, privateKey, null);
			} else if (!OUtils.equalsStringValue(userManager.getSignupPrivateKey(name), privateKey)) {
				userManager.updateSignupKey(name, privateKey);
			}	
		} else {
			throw new IllegalArgumentException("Unknown signup method was used: " + signupMethod); 
		}
		
		deleteLoginIfPresent(name, purpose);
		// here all logins are deleted
		return generateNewLogin(name, ownKeyPair, userDetails, purpose);
	}


	private KeyPair validateLoginPwd(String pwd, OpObject signupObj) throws FailedVerificationException {
		String algo = signupObj.getStringValue(OpBlockchainRules.F_ALGO);
		String keyGen = signupObj.getStringValue(OpBlockchainRules.F_KEYGEN_METHOD);
		String salt = signupObj.getStringValue(OpBlockchainRules.F_SALT);
		String sPubKey = signupObj.getStringValue(OpBlockchainRules.F_PUBKEY);
		KeyPair newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd, false);
		KeyPair ownKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, null, sPubKey);
		// the user password matches pwd specified in the blockchain
		if (!SecUtils.validateKeyPair(SecUtils.ALGO_EC, newKeyPair.getPrivate(), ownKeyPair.getPublic())) {
			throw new IllegalStateException("Specified password is wrong");
		}
		return newKeyPair;
	}
	
	private void validateOAuthMethod(OAuthUserDetails oAuthUserDetails, OpObject signupObj) {
		if (oAuthUserDetails == null) {
			throw new IllegalStateException("User was not signed up with oauth");
		}
		String salt = signupObj.getStringValue(OpBlockchainRules.F_SALT);
		String exHash = signupObj.getStringValue(OpBlockchainRules.F_OAUTHID_HASH);
		String oAuthHash = SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, salt, oAuthUserDetails.oauthUid);
		if (!OUtils.equals(oAuthHash, exHash) || !oAuthUserDetails.oauthProvider
				.equals(signupObj.getStringValue(OpBlockchainRules.F_OAUTH_PROVIDER))) {
			throw new IllegalStateException("User was signed up with another oauth method");
		}
	}

	
	@PostMapping(path = "/user-logout")
	@ResponseBody
	public ResponseEntity<String> logout(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = false, defaultValue = DEFAULT_PURPOSE_LOGIN) String purpose)
			throws FailedVerificationException {
		name = stdNickName(name);
		String spk = userManager.getSignupPrivateKey(name);
		OAuthUserDetails oauth = userManager.getOAuthLatestLogin(name);
		if (spk == null && oauth == null) {
			throw new IllegalStateException("User is not registered on the website.");
		}
		OpOperation op = deleteLoginIfPresent(name, purpose);
		if (op == null) {
			throw new IllegalArgumentException("There is nothing to edit cause login obj doesn't exist");
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(op));
	}

	@PostMapping(path = "/user-signup")
	@ResponseBody
	public ResponseEntity<String> signup(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = true) String email, @RequestParam(required = false) String pwd,
			// @RequestParam(required = false) String privateKey, @RequestParam(required = false) String publicKey,
			@RequestParam(required = false) String oauthAccessToken,
			@RequestParam(required = false) String userDetails) throws FailedVerificationException {
		name = stdNickName(name);
		if (!OpBlockchainRules.validateNickname(name)) {
			throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
		}
		if (OUtils.isEmpty(email)) {
			throw new IllegalArgumentException("Email is required for signup");
		}
		if (OUtils.isEmpty(pwd) && OUtils.isEmpty(oauthAccessToken)) {
			throw new IllegalArgumentException("Signup method is not specified");
		}
		OpObject loginObj = manager.getLoginObj(name);
		if (loginObj != null) {
			throw new UnsupportedOperationException("User is already registered, please use login method");
		}
		OpOperation signupOp = new OpOperation();
		signupOp.setType(OpBlockchainRules.OP_SIGNUP);
		OpObject obj = new OpObject();
		signupOp.addCreated(obj);
		obj.setId(name);
		if (!OUtils.isEmpty(userDetails)) {
			obj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
		}
		String algo = SecUtils.ALGO_EC;
		KeyPair newKeyPair = null;
		String sKeyPair = null;
		boolean oauthEmailVerified = false;
		OAuthUserDetails oauthUserDetails = null;
		if (!OUtils.isEmpty(pwd)) {
			obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
			algo = SecUtils.ALGO_EC;
			String salt = name;
			String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
			newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd, true);
			obj.putStringValue(OpBlockchainRules.F_SALT, salt);
			obj.putStringValue(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
			sKeyPair = SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPrivate());
		} else if (!OUtils.isEmpty(oauthAccessToken)) {
			oauthUserDetails = userOAuthController.getUserDetails(session);
			if(oauthUserDetails == null || !oauthUserDetails.accessToken.equals(oauthAccessToken)) {
				throw new IllegalArgumentException("User wasn't registered in OAuth.");
			}
			String oauthEmail = (String) oauthUserDetails.details.get(OAuthUserDetails.KEY_EMAIL);
			if (!OUtils.isEmpty(oauthEmail)) {
				if (!OUtils.equals(email, oauthEmail)) {
					throw new IllegalArgumentException("Provided user email doesn't match oauth email.");
				} else {
					oauthEmailVerified = true;
				}
			}
			obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_OAUTH);
			obj.putStringValue(OpBlockchainRules.F_SALT, name);
			obj.putStringValue(OpBlockchainRules.F_OAUTHID_HASH,
					SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, name, oauthUserDetails.oauthUid));
			obj.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthUserDetails.oauthProvider);
		}
		if (newKeyPair != null) {
			obj.putStringValue(OpBlockchainRules.F_ALGO, algo);
			obj.putStringValue(OpBlockchainRules.F_PUBKEY,
					SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPublic()));
		}
		if (oauthEmailVerified) {
			signupOp.setSignedBy(manager.getServerUser());
			manager.generateHashAndSign(signupOp, manager.getServerLoginKeyPair());
			manager.addOperation(signupOp);
			String href = getServerUrl();
			sendEmail(name, email, href, "Signup to OpenPlaceReviews", getSignupWelcomeEmailContent(name, href).toString());
			userManager.createNewUser(name, email, null, oauthUserDetails, sKeyPair, signupOp);
			return ResponseEntity.ok(formatter.fullObjectToJson(signupOp));
			// return generateNewLogin(name, ownKeyPair, userDetails, purpose);
		} else {
			String emailToken = generateEmailToken();
			String href = getServerUrl() + authUrl +"?op=signup_confirm&name=" + name + "&token=" + emailToken;
			sendEmail(name, email, href, "Signup to OpenPlaceReviews", getSignupEmailContent(name, href).toString());
			userManager.createNewUser(name, email, emailToken, oauthUserDetails, sKeyPair, signupOp);
			return ResponseEntity.ok(formatter.fullObjectToJson(signupOp));
		}
	}
	
	private String stdNickName(String name) {
		return name.trim(); // reduce errors by having trailing spaces
	}





	private OpOperation deleteLoginIfPresent(String name, String purpose) throws FailedVerificationException {
		OpOperation op = new OpOperation();
		op.setType(OpBlockchainRules.OP_LOGIN);
		OpObject loginObj = manager.getLoginObj(name + ":" + purpose);
		if (loginObj == null) {
			return null;
		} else {
			op.addDeleted(loginObj.getId());
			op.putStringValue(OpObject.F_TIMESTAMP_ADDED, OpObject.dateFormat.format(new Date()));
		}
		String serverUser = manager.getServerUser();
		String sPrivKey = userManager.getSignupPrivateKey(name);
		if (sPrivKey != null) {
			KeyPair ownKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, sPrivKey, null);
			op.setSignedBy(name);
			op.addOtherSignedBy(serverUser);
			manager.generateHashAndSign(op, ownKeyPair, manager.getServerLoginKeyPair());
		} else {
			op.setSignedBy(serverUser);
			manager.generateHashAndSign(op, manager.getServerLoginKeyPair());
		}
		manager.addOperation(op);
		userManager.updateLoginKey(name, null, purpose);
		return op;
	}
	
	private ResponseEntity<String> generateNewLogin(String name, KeyPair ownKeyPair, String userDetails, String purpose) throws FailedVerificationException {
		String serverUser = manager.getServerUser();
		KeyPair serverSignedKeyPair = manager.getServerLoginKeyPair();
		// generate & save login private key
		KeyPair loginPair = SecUtils.generateRandomEC256K1KeyPair();
		String privateKey = SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate());
		userManager.updateLoginKey(name, privateKey, purpose);

		// create login object & store in blockchain
		OpOperation loginOp = new OpOperation();
		Map<String, Object> refs = new TreeMap<String, Object>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));
		loginOp.putStringValue(OpObject.F_TIMESTAMP_ADDED, OpObject.dateFormat.format(new Date()));
		loginOp.putObjectValue(OpOperation.F_REF, refs);
		loginOp.setType(OpBlockchainRules.OP_LOGIN);
		OpObject loginObj = new OpObject();
		loginOp.addCreated(loginObj);
		if (!OUtils.isEmpty(userDetails)) {
			loginObj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
		}
		loginObj.setId(name, purpose);
		loginObj.putStringValue(OpBlockchainRules.F_ALGO, SecUtils.ALGO_EC);
		loginObj.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
		
		if(ownKeyPair != null) {
			loginOp.setSignedBy(name);
			loginOp.addOtherSignedBy(serverUser);
			manager.generateHashAndSign(loginOp, ownKeyPair, serverSignedKeyPair);
		} else {
			loginOp.setSignedBy(serverUser);
			manager.generateHashAndSign(loginOp, serverSignedKeyPair);
		}
		manager.addOperation(loginOp);
//		OpOperation copyLoygin = new OpOperation(loginOp, true);
		loginOp.putCacheObject(OpBlockchainRules.F_PRIVATEKEY, privateKey);
		return ResponseEntity.ok(formatter.fullObjectToJson(loginOp));
	}

	private String getServerUrl() {
		if (OUtils.isEmpty(serverUrl)) {
			throw new IllegalStateException("Server callback url to confirm email is not configured");
		}
		return serverUrl;
	}

	private void sendEmail(String nickname, String email, String href, String topicStr, String contentStr) {
		if (OUtils.isEmpty(sendGridApiKey)) {
			// allow test server
			LOGGER.info("Email server is not configured to send emailToken: " + href);
			return;
		}
		
		if (sendGridClient == null) {
			sendGridClient = new SendGrid(sendGridApiKey);
		}
		LOGGER.info("Sending mail to: " + email);
		Email from = new Email("noreply@openplacereviews.org");
		from.setName("OpenPlaceReviews");
		Email to = new Email(email);
		
		Content content = new Content("text/html", contentStr);
		Mail mail = new Mail(from, topicStr, to, content);
		mail.from = from;
		// mail.setTemplateId(templateId);
		// MailSettings mailSettings = new MailSettings();
		// FooterSetting footerSetting = new FooterSetting();
		// footerSetting.setEnable(true);
		// String footer = String.format("<center style='margin:5px 0px 5px 0px'>"
		// + "<h2>Your promocode is '%s'.</h2></center>", promocodes) ;
		// footerSetting.setHtml("<html>"+footer+"</html>");
		// mailSettings.setFooterSetting(footerSetting);
		// mail.setMailSettings(mailSettings);

		Request request = new Request();
		try {
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");
			String body = mail.build();
			request.setBody(body);
			Response response = sendGridClient.api(request);
			LOGGER.info("Response code: " + response.getStatusCode());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
	}

	private StringBuilder getSignupEmailContent(String nickname, String href) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Hello <b>%s</b> and welcome to OpenPlaceReviews!", nickname));
		sb.append("<br><br>");
		
		sb.append(String.format("To finish registration please confirm your email by following the link "
				+ "<a href=\"%s\">%s</a>.", href, href));
		sb.append("<br><br>Best Regards, <br> OpenPlaceReviews");
		return sb;
	}
	
	
	private StringBuilder getSignupWelcomeEmailContent(String nickname, String href) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Hello <b>%s</b> and welcome to OpenPlaceReviews!", nickname));
		sb.append("<br><br>");
		
		sb.append(String.format("You can contribute to OpenPlaceReviews by following this link"
				+ "<a href=\"%s\">%s</a>.", href, href));
		sb.append("<br><br>Best Regards, <br> OpenPlaceReviews");
		return sb;
	}
	
	private StringBuilder getResetEmailContent(String nickname, String href, String emailToken) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Hello <b>%s</b>!", nickname));
		sb.append("<br><br>");
		
		sb.append(String.format("You '%s' have requested to reset the password, please follow the link "
				+ "<a href=\"%s\">%s</a> (reset token '%s').", nickname, href, href, emailToken));
		sb.append("<br><br>Best Regards, <br> OpenPlaceReviews");
		return sb;
	}
}
