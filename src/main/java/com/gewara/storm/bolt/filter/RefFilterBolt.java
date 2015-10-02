/**  
* @Project: hawk
* @Title: RefFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Mar 27, 2014 3:23:50 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gewara.constant.ConfigProps;
import com.gewara.constant.UVEnum;
 

public class RefFilterBolt extends BaseFilterBolt {
	private static final long serialVersionUID = 1L;

	private Pattern sem = Pattern.compile(ConfigProps.REGEX_SEM, Pattern.CASE_INSENSITIVE);
	private Pattern seo = Pattern.compile(ConfigProps.REGEX_SEO, Pattern.CASE_INSENSITIVE);
	private Pattern direct = Pattern.compile(ConfigProps.REGEX_DIRECT, Pattern.CASE_INSENSITIVE);
	@Override
	public boolean accept(Map map) {
        if(map!=null){
        	String ref=(String)map.get(UVEnum.ref.name()); 
        	String cookie=(String)map.get(UVEnum.sem.name()); 
        	ref = ref==null?"":ref;
        	cookie = cookie==null?"":cookie;
        	//SEM
    		Matcher semMatcher = sem.matcher(cookie);
    		if(semMatcher.find()){
    			map.put(UVEnum.reftype.name(), ConfigProps.TYPE_REF_SEM);
            	return true;
    		}
        	//SEO
    		Matcher seoMatcher = seo.matcher(ref);
    		if(seoMatcher.find()){
    			map.put(UVEnum.reftype.name(), ConfigProps.TYPE_REF_SEO);
            	return true;
    		}
    		//Direct
    		Matcher directMatcher = direct.matcher(ref);
    		if(directMatcher.find()){
    			map.put(UVEnum.reftype.name(), ConfigProps.TYPE_REF_DIRECT);
            	return true;
    		}
    		//Referral
    		map.put(UVEnum.reftype.name(), ConfigProps.TYPE_REF_REFERRAL);
			return true;
        }
		return false;
	}

}
