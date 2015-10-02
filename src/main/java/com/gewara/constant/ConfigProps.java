/**  
* @Project: hawk
* @Title: Config.java
* @Package com.gewara.constant
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Apr 15, 2014 11:39:39 PM
* @version V1.0  
*/

package com.gewara.constant;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
 
public class ConfigProps{
	//zookeepr、kafka、storm相关配置
	public final static String KEY_MEMCACHE_HOST ="memchachehost";
	public final static String KEY_MEMCACHE_PORT ="memchacheport";
	public final static String KEY_BROKER_KAFKA ="brokerkafka";
	public final static String KEY_ZOOKEEPER_KAFKA ="zookeeperkafka";
	public final static String KEY_NIMBUS_HOST ="nimbushost";
	
    public final static String TOPIC_TABLE = "table";
	public final static String TOPIC_USER = "userlog";
	public final static String TABLE_NAME = "tablename";
	//用户标识
	public final static String SN_UV      =   "101000000001"; //流量用户日志
	public final static String SN_REG     =   "101000000002"; //注册用户日志
	public final static String SN_CONSUME =   "101000000003"; //消费用户日志
	//用户行为
	public final static String SN_DOWNLOAD =  "101000000007"; //用户下载链接日志
	public final static String SN_SHARE    =  "101000000008"; //用户分享链接日志
	
    //搜索引擎
    public static final Map<String, String> SEARCH_ENGINEE =new HashMap<String, String>();
	static{
		SEARCH_ENGINEE.put("www.baidu.com", "百度"); 
		SEARCH_ENGINEE.put("www.sogou.com", "搜狗"); 
		SEARCH_ENGINEE.put("www.google.com.hk", "谷歌"); 
		SEARCH_ENGINEE.put("www.so.com", "360搜索"); 
		SEARCH_ENGINEE.put("r.search.yahoo.com", "雅虎"); 
		SEARCH_ENGINEE.put("www.soso.com", "搜搜"); 
		SEARCH_ENGINEE.put("cn.bing.com", "必应"); 
	}
    
	public static final String TYPE_REF_SEO       ="seo";
	public static final String TYPE_REF_SEM       ="sem";
	public static final String TYPE_REF_DIRECT    ="direct";
	public static final String TYPE_REF_REFERRAL  ="referral";
	
	public static final Map<String, String> TYPE_REF =new HashMap<String, String>();
	static{
		TYPE_REF.put(TYPE_REF_SEO,       "seo搜索"); 
		TYPE_REF.put(TYPE_REF_SEM,       "sem搜索"); 
		TYPE_REF.put(TYPE_REF_DIRECT,    "直接流量"); 
		TYPE_REF.put(TYPE_REF_REFERRAL,  "引荐流量"); 
	}
    public final static String REGEX_SEO = "^\\s*http://"+StringUtils.join(SEARCH_ENGINEE.keySet(), "|^\\s*http://");
    public final static String REGEX_SEM = "utm_medium=.*cpc.*|utmcmd=.*cpc.*";
    public final static String REGEX_DIRECT = "^\\s*http://www.gewara.com";
	    
	 
	private Map<String, String> configMap = new HashMap<String, String>();
	public String getString(String key){
		String result = configMap.get(key);
		if(StringUtils.isBlank(result)){
			result = configMap.get(key)==null?null: configMap.get(key);
		}
		return result;
	}
	public Long getLong(String key){
		String result = getString(key);
		if(StringUtils.isBlank(result)) return null;
		return Long.valueOf(result);
	}
	public Integer getInteger(String key){
		String result = getString(key);
		if(StringUtils.isBlank(result)) return null;
		return Integer.valueOf(result);
	}
	public void put(String key, String value){
		configMap.put(key, value);
	}
	
	public static Map<String, String> getServerAddrMap(){
		Map<String, String> hostMap = new TreeMap<String, String>();
		try{
			Enumeration<NetworkInterface> niList = NetworkInterface.getNetworkInterfaces();
			while(niList.hasMoreElements()){
				NetworkInterface ni = niList.nextElement();
				Enumeration<InetAddress> addrList = ni.getInetAddresses();
				while(addrList.hasMoreElements()){
					InetAddress addr = addrList.nextElement();
					if(addr instanceof Inet4Address) {//只做IPV4
						hostMap.put(addr.getHostAddress(), addr.getHostName());
					}
				}
			}
		}catch(Exception e){
		}
		return hostMap;
	}

}
