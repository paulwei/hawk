/**  
* @Project: hawk
* @Title: TableFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: 消息体表名过滤器
* @author honglin.wei@gewara.com
* @date Mar 24, 2014 11:29:02 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.gewara.constant.ConfigProps;


 
public class TableFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 2098742886120671261L;
	private String tablename;
	
	public TableFilterBolt(String tablename) {
		this.tablename = tablename;
	}

	@Override
	public boolean accept(Map map) {
		  if(map!=null && StringUtils.isNotBlank(tablename) && tablename.equals(map.get(ConfigProps.TABLE_NAME))){
	        	return true;
	        }
			return false;
	}

}
