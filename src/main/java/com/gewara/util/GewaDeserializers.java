/**  
* @Project: hawk
* @Title: GewaDeserializers.java
* @Package com.gewara.util
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Mar 26, 2014 10:12:31 PM
* @version V1.0  
*/

package com.gewara.util;

import java.sql.Timestamp;

import org.codehaus.jackson.map.module.SimpleDeserializers;

public class GewaDeserializers extends SimpleDeserializers {
	public GewaDeserializers(){
		addDeserializer(Timestamp.class, new GewaTimestampDeserializer());
	}
}
