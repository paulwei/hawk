/**  
* @Project: hawk
* @Title: GewaTimestampDeserializer.java
* @Package com.gewara.util
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Mar 26, 2014 10:13:46 PM
* @version V1.0  
*/

package com.gewara.util;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.deser.TimestampDeserializer;
public class GewaTimestampDeserializer extends TimestampDeserializer {
	protected java.util.Date _parseDate(JsonParser jp, DeserializationContext ctxt) 
			throws IOException, JsonProcessingException {
		JsonToken t = jp.getCurrentToken();
		try {
			if (t == JsonToken.VALUE_NUMBER_INT) {
				return new java.util.Date(jp.getLongValue());
			}
			if (t == JsonToken.VALUE_STRING) {
				/*
				 * As per [JACKSON-203], take empty Strings to mean null
				 */
				String str = jp.getText().trim();
				if (str.length() == 0) {
					return null;
				}
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				return formatter.parse(str);
			}
			throw ctxt.mappingException(_valueClass);
		} catch (ParseException e) {
			throw ctxt.mappingException(_valueClass);
		} catch (IllegalArgumentException iae) {
			throw ctxt.weirdStringException(_valueClass,
					"not a valid representation (error: " + iae.getMessage()
							+ ")");
		}
	}
}
