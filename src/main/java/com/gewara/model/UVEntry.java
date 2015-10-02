/**  
 * @Project: hawk
 * @Title: UvEntry.java
 * @Package com.gewara.model
 * @Description: 用户访问日志实体
 * @author honglin.wei@gewara.com
 * @date Mar 21, 2014 10:25:14 AM
 * @version V1.0  
 */

package com.gewara.model;

import java.sql.Timestamp;

public class UVEntry {
	private String sn;     //采集系统分配
	private String pkey;   //项目id
	private String uagent; //终端类型（web/wap/ios/android）
	private String uasn;   //cookie _gw_uasn中的值
	private String sem;    //sem中的值
	private String ref;    //链接地址来源
	private String uid;    //用户ID
	private String uip;    //用户IP
	private String url;    //访问URL
	private Timestamp timestamp; //记录时间戳

	
	public String getSem() {
		return sem;
	}

	public void setSem(String sem) {
		this.sem = sem;
	}

	public UVEntry(String sn, String pkey, String uagent, String uasn, Timestamp timestamp ) {
		this.sn = sn;
		this.pkey = pkey;
		this.uagent = uagent;
		this.uasn = uasn;
		this.timestamp = timestamp;
	}

	public String getSn() {
		return sn;
	}

	public void setSn(String sn) {
		this.sn = sn;
	}

	public String getPkey() {
		return pkey;
	}

	public void setPkey(String pkey) {
		this.pkey = pkey;
	}

	public String getUagent() {
		return uagent;
	}

	public void setUagent(String uagent) {
		this.uagent = uagent;
	}

	public String getUasn() {
		return uasn;
	}

	public void setUasn(String uasn) {
		this.uasn = uasn;
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getUip() {
		return uip;
	}

	public void setUip(String uip) {
		this.uip = uip;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}


}
