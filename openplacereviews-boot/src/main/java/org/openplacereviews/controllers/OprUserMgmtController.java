package org.openplacereviews.controllers;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.UserSchemaManager;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
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
	
	// TODO AUTH methods:
	// login status ? 
	// resend email
	// oauth
	
	// TODO resend email after expiration
	// TODO give proper error after login (remove email token)
	// TODO reset password

	// TODO !!! WebSecurityConfiguration !!!
	// TODO allow register with private key after oauth
	// TODO Situation that signup is pending email verification
	//   ? allow signup if token expired here we could check that verification email expired and delete after 24 h
	// TODO check that email verification is pending
	protected static final Log LOGGER = LogFactory.getLog(OprUserMgmtController.class);

	private static final String PURPOSE_LOGIN = "opr-web";
	
	@Value("${opendb.email.sendgrid-api}")
	private String sendGridApiKey;

	@Value("${opendb.serverUrl}")
	private String serverUrl;

	@Autowired
	private BlocksManager manager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private UserSchemaManager userManager;

	private SendGrid sendGridClient;

	@GetMapping(path = "/user-signup-confirm")
	@ResponseBody
	public ResponseEntity<String> signupConfirm(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = true) String token, 
			@RequestParam(required = false) String userDetails) throws FailedVerificationException {

		OpOperation signinOp = userManager.validateEmail(name, token);
		String sPrivKey = userManager.getSignupPrivateKey(name);
		String sPubKey = signinOp.getCreated().get(0).getStringValue(OpBlockchainRules.F_PUBKEY);
		KeyPair ownKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, sPrivKey, sPubKey);
		// String pubKey = SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPublic());
		// op.setSignedBy(signName);
		String serverUser = manager.getServerUser();
		KeyPair serverSignedKeyPair = manager.getServerLoginKeyPair();
		signinOp.setSignedBy(name);
		signinOp.addOtherSignedBy(serverUser);
		manager.generateHashAndSign(signinOp, ownKeyPair, serverSignedKeyPair);
		manager.addOperation(signinOp);
		
		return generateNewLogin(name, ownKeyPair, userDetails);
	}
	
	@PostMapping(path = "/user-login")
	@ResponseBody
	public ResponseEntity<String> userLogin(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = true) String pwd, 
			@RequestParam(required = false) String email, @RequestParam(required = false) String userDetails) throws FailedVerificationException {
		OpObject signupObj = manager.getLoginObj(name);
		if (signupObj == null) {
			throw new IllegalStateException("User was not signed up");
		}
		String algo = signupObj.getStringValue(OpBlockchainRules.F_ALGO);
		String keyGen = signupObj.getStringValue(OpBlockchainRules.F_KEYGEN_METHOD);
		String salt = signupObj.getStringValue(OpBlockchainRules.F_SALT);
		String sPubKey = signupObj.getStringValue(OpBlockchainRules.F_PUBKEY);
		KeyPair newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
		KeyPair ownKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, null, sPubKey);
		if (!SecUtils.validateKeyPair(SecUtils.ALGO_EC, newKeyPair.getPrivate(), ownKeyPair.getPublic())) {
			throw new IllegalStateException("Specified password is wrong");
		}
		if (userManager.getSignupPrivateKey(name) == null) {
			userManager.createNewUser(name, null, null,
					SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPrivate()), signupObj);
		}
		deleteLoginIfPresent(name);
		return generateNewLogin(name, newKeyPair, userDetails);
	}

	private ResponseEntity<String> generateNewLogin(String name, KeyPair ownKeyPair, String userDetails) throws FailedVerificationException {
		String serverUser = manager.getServerUser();
		KeyPair serverSignedKeyPair = manager.getServerLoginKeyPair();
		// generate & save login private key
		KeyPair loginPair = SecUtils.generateRandomEC256K1KeyPair();
		String privateKey = SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate());
		userManager.createNewLogin(name, privateKey);

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
		loginObj.setId(name, PURPOSE_LOGIN);
		loginObj.putStringValue(OpBlockchainRules.F_ALGO, SecUtils.ALGO_EC);
		loginObj.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
		
		loginOp.setSignedBy(name);
		loginOp.addOtherSignedBy(serverUser);
		manager.generateHashAndSign(loginOp, ownKeyPair, serverSignedKeyPair);
		manager.addOperation(loginOp);
		loginOp.putCacheObject(OpBlockchainRules.F_PRIVATEKEY, privateKey);
		return ResponseEntity.ok(formatter.fullObjectToJson(loginOp));
	}
	
	@PostMapping(path = "/user-logout")
	@ResponseBody
	public ResponseEntity<String> logout(HttpSession session, @RequestParam(required = true) String name) throws FailedVerificationException {
		OpOperation op = deleteLoginIfPresent(name);
		if(op == null) {
			throw new IllegalArgumentException("There is nothing to edit cause login obj doesn't exist");
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(op));
	}

	private OpOperation deleteLoginIfPresent(String name) throws FailedVerificationException {
		OpOperation op = new OpOperation();
		op.setType(OpBlockchainRules.OP_LOGIN);
		OpObject loginObj = manager.getLoginObj(name + ":" + PURPOSE_LOGIN);
		if (loginObj == null) {
			return null;
		} else {
			op.addDeleted(loginObj.getId());
			op.putStringValue(OpObject.F_TIMESTAMP_ADDED, OpObject.dateFormat.format(new Date()));
		}
		String serverUser = manager.getServerUser();
		String sPrivKey = userManager.getSignupPrivateKey(name);
		KeyPair ownKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, sPrivKey, null);
		KeyPair serverSignedKeyPair = manager.getServerLoginKeyPair();
		op.setSignedBy(name);
		op.addOtherSignedBy(serverUser);
		manager.generateHashAndSign(op, ownKeyPair, serverSignedKeyPair);
		manager.addOperation(op);
		userManager.removeLogin(name);
		return op;
	}

	@PostMapping(path = "/user-signup")
	@ResponseBody
	public ResponseEntity<String> signup(HttpSession session, @RequestParam(required = true) String name,
			@RequestParam(required = true) String email, @RequestParam(required = false) String pwd,
			// @RequestParam(required = false) String privateKey, @RequestParam(required = false) String publicKey,
			@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId,
			@RequestParam(required = false) String userDetails) throws FailedVerificationException {
		name = name.trim(); // reduce errors by having trailing spaces
		if (!OpBlockchainRules.validateNickname(name)) {
			throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
		}
		if (OUtils.isEmpty(pwd) && OUtils.isEmpty(oauthId)) {
			throw new IllegalArgumentException("Signup method is not specified");
		}
		OpObject loginObj = manager.getLoginObj(name);
		if (loginObj != null) {
			throw new UnsupportedOperationException("User is already registered");
		}
		OpOperation op = new OpOperation();
		op.setType(OpBlockchainRules.OP_SIGNUP);
		OpObject obj = new OpObject();
		op.addCreated(obj);
		obj.setId(name);
		if (!OUtils.isEmpty(userDetails)) {
			obj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
		}
		String algo = SecUtils.ALGO_EC;
		KeyPair newKeyPair = null;
		String sKeyPair = null;
		if (!OUtils.isEmpty(pwd)) {
			obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
			algo = SecUtils.ALGO_EC;
			String salt = name;
			String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
			newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
			obj.putStringValue(OpBlockchainRules.F_SALT, salt);
			obj.putStringValue(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
			sKeyPair = SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPrivate());
		} else if (!OUtils.isEmpty(oauthId)) {
			obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_OAUTH);
			obj.putStringValue(OpBlockchainRules.F_SALT, name);
			obj.putStringValue(OpBlockchainRules.F_OAUTHID_HASH,
					SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, name, oauthId));
			obj.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthProvider);
		}
		if (newKeyPair != null) {
			obj.putStringValue(OpBlockchainRules.F_ALGO, algo);
			obj.putStringValue(OpBlockchainRules.F_PUBKEY,
					SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPublic()));
		}
		String emailToken = UUID.randomUUID().toString();
		sendEmail(name, email, emailToken);
		userManager.createNewUser(name, email, emailToken, sKeyPair, op);
		return ResponseEntity.ok(formatter.fullObjectToJson(op));
	}

	private void sendEmail(String nickname, String email, String emailToken) {
		if (OUtils.isEmpty(serverUrl)) {
			throw new IllegalStateException("Server callback url to confirm email is not configured");
		}
		String href = serverUrl + "api/auth/user-signup-confirm?name=" + nickname + "&token=" + emailToken;
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
		
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Hello <b>%s</b> and welcome to OpenPlaceReviews!", nickname));
		sb.append("<br><br>");
		
		sb.append(String.format("To finish registration please confirm your email by following the link "
				+ "<a href=\"%s\">%s</a>.", href, href));
		sb.append("<br><br>Best Regards, <br> OpenPlaceReviews");
		Content content = new Content("text/html", sb.toString());
		Mail mail = new Mail(from, "Please confirm your signup to OpenPlaceReviews", 
				to, content);
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
}
