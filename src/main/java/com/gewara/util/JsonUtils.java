/**  
 * @Project: hawk
 * @Title: JsonUtils.java
 * @Package com.gewara.util
 * @Description: TODO
 * @author honglin.wei@gewara.com
 * @date Mar 26, 2014 10:08:03 PM
 * @version V1.0  
 */

package com.gewara.util;

import java.io.OutputStream;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;
import org.codehaus.jackson.map.type.CollectionType;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {
	private static final transient Logger logger = LoggerFactory.getLogger(JsonUtils.class);

	public static <T> T readJsonToObject(Class<T> clazz, String json) {
		if (StringUtils.isBlank(json))
			return null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.getDeserializerProvider().withAdditionalDeserializers(new GewaDeserializers());
		try {
			T result = mapper.readValue(json, clazz);
			return result;
		} catch (Exception e) {
			logger.error(json, e);
		}
		return null;
	}

	public static <T> T readJsonToObject(TypeReference<T> type, String json) {
		if (StringUtils.isBlank(json))
			return null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.getDeserializerProvider().withAdditionalDeserializers(new GewaDeserializers());
		try {
			T result = mapper.readValue(json, type);
			return result;
		} catch (Exception e) {
			logger.error(json, e);
		}
		return null;
	}

	public static <T> List<T> readJsonToObjectList(Class<T> clazz, String json) {
		if (StringUtils.isBlank(json))
			return null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.getDeserializerProvider().withAdditionalDeserializers(new GewaDeserializers());

		try {
			CollectionType type = mapper.getTypeFactory().constructCollectionType(List.class, clazz);
			List<T> result = mapper.readValue(json, type);
			return result;
		} catch (Exception e) {
			logger.error("json:" + json, e);
		}
		return null;
	}

	public static Map readJsonToMap(String json) {
		if (StringUtils.isBlank(json))
			return new HashMap();
		ObjectMapper mapper = new ObjectMapper();
		mapper.getDeserializerProvider().withAdditionalDeserializers(new GewaDeserializers());
		try {
			Map result = mapper.readValue(json, Map.class);
			if (result == null)
				result = new HashMap();
			return result;
		} catch (Exception e) {
			logger.error("json:" + json, e);
			return new HashMap();
		}

	}

	public static String writeObjectToJson(Object object) {
		return writeObjectToJson(object, false);
	}

	public static void writeObjectToStream(OutputStream os, Object object, boolean excludeNull) {
		writeObject(object, os, null, excludeNull);
	}

	public static void writeObjectToWriter(Writer writer, Object object, boolean excludeNull) {
		writeObject(object, null, writer, excludeNull);
	}

	public static String writeObjectToJson(Object object, boolean excludeNull) {
		return writeObject(object, null, null, excludeNull);
	}

	private static String writeObject(Object object, OutputStream os, Writer writer, boolean excludeNull) {
		if (object == null)
			return null;
		if (object instanceof Map) {
			try {
				((Map) object).remove(null);
			} catch (Exception e) {
			}
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
		if (excludeNull) {
			mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
		}

		try {
			CustomSerializerFactory sf = new CustomSerializerFactory();
			sf.addGenericMapping(Date.class, new GewaDateSerializer());
			mapper.setSerializerFactory(sf);
			if (os != null) {
				mapper.writeValue(os, object);
			} else if (writer != null) {
				mapper.writeValue(writer, object);
			} else {
				String data = mapper.writeValueAsString(object);
				return data;
			}
		} catch (Exception e) {
			logger.error("trace", e);
		}
		return null;
	}

	public static String writeMapToJson(Map<String, String> dataMap) {
		if (dataMap == null)
			return null;
		if (dataMap instanceof HashMap) {
			try {
				dataMap.remove(null);
			} catch (Exception e) {
			}
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
		try {
			String data = mapper.writeValueAsString(dataMap);
			return data;
		} catch (Exception e) {
			logger.error("", e);
		}
		return null;
	}

	public static String addJsonKeyValue(String json, String key, String value) {
		Map info = readJsonToMap(json);
		info.put(key, value);
		return writeMapToJson(info);
	}

	public static String removeJsonKeyValue(String json, String key) {
		Map info = readJsonToMap(json);
		info.remove(key);
		return writeMapToJson(info);
	}

	public static String getJsonValueByKey(String json, String key) {
		Map<String, String> info = readJsonToMap(json);
		return info.get(key);
	}

}