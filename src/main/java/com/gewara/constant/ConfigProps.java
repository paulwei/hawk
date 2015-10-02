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
	//zookeepr��kafka��storm�������
	public final static String KEY_MEMCACHE_HOST ="memchachehost";
	public final static String KEY_MEMCACHE_PORT ="memchacheport";
	public final static String KEY_BROKER_KAFKA ="brokerkafka";
	public final static String KEY_ZOOKEEPER_KAFKA ="zookeeperkafka";
	public final static String KEY_NIMBUS_HOST ="nimbushost";
	
    public final static String TOPIC_TABLE = "table";
	public final static String TOPIC_USER = "userlog";
	public final static String TABLE_NAME = "tablename";
	//�û���ʶ
	public final static String SN_UV      =   "101000000001"; //�����û���־
	public final static String SN_REG     =   "101000000002"; //ע���û���־
	public final static String SN_CONSUME =   "101000000003"; //�����û���־
	//�û���Ϊ
	public final static String SN_DOWNLOAD =  "101000000007"; //�û�����������־
	public final static String SN_SHARE    =  "101000000008"; //�û�����������־
	
    //��������
    public static final Map<String, String> SEARCH_ENGINEE =new HashMap<String, String>();
	static{
		SEARCH_ENGINEE.put("www.baidu.com", "�ٶ�"); 
		SEARCH_ENGINEE.put("www.sogou.com", "�ѹ�"); 
		SEARCH_ENGINEE.put("www.google.com.hk", "�ȸ�"); 
		SEARCH_ENGINEE.put("www.so.com", "360����"); 
		SEARCH_ENGINEE.put("r.search.yahoo.com", "�Ż�"); 
		SEARCH_ENGINEE.put("www.soso.com", "����"); 
		SEARCH_ENGINEE.put("cn.bing.com", "��Ӧ"); 
	}
    
	public static final String TYPE_REF_SEO       ="seo";
	public static final String TYPE_REF_SEM       ="sem";
	public static final String TYPE_REF_DIRECT    ="direct";
	public static final String TYPE_REF_REFERRAL  ="referral";
	
	public static final Map<String, String> TYPE_REF =new HashMap<String, String>();
	static{
		TYPE_REF.put(TYPE_REF_SEO,       "seo����"); 
		TYPE_REF.put(TYPE_REF_SEM,       "sem����"); 
		TYPE_REF.put(TYPE_REF_DIRECT,    "ֱ������"); 
		TYPE_REF.put(TYPE_REF_REFERRAL,  "��������"); 
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
					if(addr instanceof Inet4Address) {//ֻ��IPV4
						hostMap.put(addr.getHostAddress(), addr.getHostName());
					}
				}
			}
		}catch(Exception e){
		}
		return hostMap;
	}

}
