package org.openplacereviews.osm.parser;

import com.google.openlocationcode.OpenLocationCode;
import org.apache.commons.lang3.RandomStringUtils;
import org.openplacereviews.osm.model.LatLon;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.google.openlocationcode.OpenLocationCode.CodeArea;

public class OsmLocationTool {
	private static final int CODE_LENGTH = 6;

	private static final int ID_LENGTH = CODE_LENGTH + 4;

	/**
	 * Encode latitude and longitude in compact {@link OsmLocationTool#CODE_LENGTH} length string
	 * @param latitude
	 * @param longitude
	 * @return
	 */
	public static String encode(double latitude, double longitude) {
		String code = OpenLocationCode.encode(latitude, longitude, CODE_LENGTH);
		return code.substring(0, CODE_LENGTH);
	}

	public static String encode(double latitude, double longitude, int codeLength) {
		String code = OpenLocationCode.encode(latitude, longitude, codeLength);
		return code.substring(0, codeLength);
	}

	/**
	 * Decode input in {@link CodeArea} object.
	 * @param code
	 * @return
	 */
	public static CodeArea decode(String code) {
		return OpenLocationCode.decode(code + "00+");
	}

	/**
	 * Retrieve geo compact code from osm operation id
	 * @param id Osm operation id
	 * @return
	 */
	public static String codeFromId(String id) {
		return id.substring(0, CODE_LENGTH);
	}

	public static Integer intFromId(String id) {
		return Integer.parseInt(id.substring(CODE_LENGTH), 16);
	}

	/**
	 * Generate osm id from {@code latitude} and {@code longitude}</br>
	 * and random int value.
	 * @param latitude
	 * @param longitude
	 * @return
	 */
	public static String generateStrId(double latitude, double longitude) {
		String code = encode(latitude, longitude);
		Integer suffix = new Random().nextInt(Integer.MAX_VALUE);

		return code + BigInteger.valueOf(suffix).toString(16);
	}

	public static String generateStrId(LatLon latLon) {
		if (latLon == null) {
			return null;
		}
		return generateStrId(latLon.getLatitude(), latLon.getLongitude());
	}

	public static List<String> generatePlaceLocationId(LatLon latLon) {
		return generatePlaceLocationId(latLon.getLatitude(), latLon.getLongitude());
	}

	public static List<String> generatePlaceLocationId(double latitude, double longitude) {
		String firstKey = encode(latitude,longitude);
		String secondKey = RandomStringUtils.randomAlphanumeric(6).toLowerCase();

		return Arrays.asList(firstKey, secondKey);
	}

	/**
	 * Convert osm id from string to {@link byte[]} array
	 * @param id
	 * @return
	 */
	public static byte[] convertIdToBytes(String id) {
		try {
			//Get geo code byte
			byte[] prefixArrOut = id.substring(0, CODE_LENGTH).getBytes("UTF-8");
			//Get integer from string
			Integer suffixInt = Integer.parseInt(id.substring(CODE_LENGTH, id.length()), 16);
			byte[] suffixArrOut = ByteBuffer.allocate(4).putInt(suffixInt).array();

			return mergeTwoArrays(prefixArrOut, suffixArrOut);
		} catch (Exception e) {
			throw new RuntimeException("impossible");
		}
	}

	/**
	 * Convert bytes array to {@link String} osm id.
	 * @param inIdArray
	 * @return
	 */
	public static String convertBytesToId(byte[] inIdArray) {
		if (inIdArray.length != ID_LENGTH)
			throw new IllegalArgumentException("Id must be " + ID_LENGTH + " bytes");
		byte[] prefixArrOut = new byte[CODE_LENGTH];
		for (int i = 0; i < CODE_LENGTH; i++) {
			prefixArrOut[i] = inIdArray[i];
		}
		byte[] suffixArrOut = new byte[4];
		for (int i = 0; i < 4; i++) {
			suffixArrOut[i] = inIdArray[CODE_LENGTH + i];
		}

		try {
			String prefix = new String(prefixArrOut, "UTF-8");
			int suffix = ByteBuffer.wrap(suffixArrOut).getInt();

			return prefix + BigInteger.valueOf(suffix).toString(16);
		} catch (Exception e) {
			throw new RuntimeException("Impossible unsupported encoding");
		}
	}

	/**
	 * Merge two {@link byte[]}
	 * @param b1
	 * @param b2
	 * @return
	 */
	public static byte[] mergeTwoArrays(byte[] b1, byte[] b2) {
		byte[] m = b1 == null ? b2 : b1;
		if (b2 != null && b1 != null) {
			m = new byte[b1.length + b2.length];
			System.arraycopy(b1, 0, m, 0, b1.length);
			System.arraycopy(b2, 0, m, b1.length, b2.length);
		}
		return m;
	}

}
