/**  
 * @Project: hawk
 * @Title: UvEntry.java
 * @Package com.gewara.model
 * @Description: �û�������־ʵ��
 * @author honglin.wei@gewara.com
 * @date Mar 21, 2014 10:25:14 AM
 * @version V1.0  
 */

package com.gewara.model;

import java.sql.Timestamp;

public class UVEntry {
	private String sn;     //�ɼ�ϵͳ����
	private String pkey;   //��Ŀid
	private String uagent; //�ն����ͣ�web/wap/ios/android��
	private String uasn;   //cookie _gw_uasn�е�ֵ
	private String sem;    //sem�е�ֵ
	private String ref;    //���ӵ�ַ��Դ
	private String uid;    //�û�ID
	private String uip;    //�û�IP
	private String url;    //����URL
	private Timestamp timestamp; //��¼ʱ���

	
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
