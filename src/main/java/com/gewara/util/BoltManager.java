/**  
* @Project: hawk
* @Title: BoltManager.java
* @Package com.gewara.util
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Mar 31, 2014 12:18:12 PM
* @version V1.0  
*/

package com.gewara.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ClassName BoltManager
 * @Description TODO
 * @author weihonglin pau.wei2011@gmail.com
 * @date Mar 31, 2014
 */

public class BoltManager {
	public static final Map<String, BoltManager> manageMapper = new HashMap<String, BoltManager>();
	
	private AtomicBoolean  clear;    //��ջ�����
	private AtomicBoolean  emit;     //������
	
	public AtomicBoolean getEmit() {
		return emit;
	}

	private Long period;             //������
	private Timer clearTimer ;       //��ʱ���
	private Timer emitTimer ;        //��ʱ����
	private Date firstStartTime;     //��ʱ��ʼʱ��
	public static BoltManager prepare(String tag){
		BoltManager bm = manageMapper.get(tag);
		if(bm == null){
			bm = new BoltManager();
			bm.init();
			manageMapper.put(tag, bm);
		}
		return bm;
	}
	public void init(){
		this.firstStartTime = DateUtil.getBeginningTimeOfDay(DateUtil.addDay(DateUtil.getCurDate(), 1));
		this.period = 24*60*60*1000L;

		this.clear = new  AtomicBoolean(true);
		this.emit  = new  AtomicBoolean(true);
		this.clearTimer = new Timer();
		clearTimer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				clear.set(true);
			}
		}, firstStartTime, period);
		this.emitTimer = new Timer();
		emitTimer.schedule(new TimerTask(){
			@Override
			public void run() {
				emit.set(true);
			}
		}, 0, 60*1000);

	}

}
