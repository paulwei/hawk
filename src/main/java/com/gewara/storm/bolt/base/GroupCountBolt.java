/**  
* @Project: hawk
* @Title: GroupDistBolt.java
* @Package com.gewara.storm.bolt
* @Description: �������Bolt
* @author honglin.wei@gewara.com
* @date Mar 6, 2014 4:59:50 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.base;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.MemcachedClient;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.gewara.constant.ConfigFactory;
import com.gewara.constant.ConfigProps;
import com.gewara.constant.GroupEnum;
import com.gewara.util.DateUtil;

public class GroupCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private volatile boolean emit = true;     //������
	private Fields  groupField;      //�����ֶ�
	private Long    period;          //ͳ�Ƽ��
	private String  name;            //bolt����
 	private transient MemcachedClient memcachedClient; //����
 	
    private static final Logger logger = LoggerFactory.getLogger(GroupCountBolt.class); 
	private Map<String, Integer> countMap=new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> snapShotMap=new ConcurrentHashMap<String, Integer>();

	public GroupCountBolt(Fields groupField,String name) {
		this(groupField,name,null);
	}
	public GroupCountBolt(Fields groupField,String name,Long period) {
		this.groupField = groupField;
		this.name = name ;
		this.period = period!=null?period:(60*1000);
	}
	protected void init(){
		Date delayClear = DateUtil.getBeginningTimeOfDay(DateUtil.addDay(DateUtil.getCurDate(), 1));
		Date delayStat = DateUtil.getCurFullTimestamp();
		Timer clearTimer = new Timer();
		clearTimer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				countMap.clear();
				snapShotMap.clear();
				try {
					memcachedClient.set(name, 60 * 60 * 24 * 2, countMap);
				} catch (Exception e) {
					logger.error("memcache �쳣  bolt=" + name, e);
				}
				logger.info("clear memcache bolt="+name+",countMap.size="+countMap.size()+",snapShotMap.size"+snapShotMap.size());
			}
		}, delayClear, 24*60*60*1000);
		Timer emitTimer = new Timer();
		emitTimer.schedule(new TimerTask(){
			@Override
			public void run() {
				emit=true;
			}
		}, delayStat,period);
	}
	protected void initData(){
		try {
			String host = ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
			Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
			memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
			Map<String, Integer> m = (Map<String, Integer>)memcachedClient.get(name);
			if(m!=null){copyMap(m,countMap);}
			logger.info("[initData bolt="+name+"]memMap="+m+",countMap="+countMap);
		} catch (Exception e) {
			logger.error("memcache �쳣  bolt="+name, e);
 			e.printStackTrace();
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		initData();
		init();
		
	}
   @Override
    public void cleanup() {
	   memcachedClient.shutdown();
    }
	 
	@Override
	public void execute(Tuple input) {
		List<String> groupFieldList = groupField.toList();
		List<String> groupValueList = new ArrayList<String>();
		for(String field:groupFieldList){
			String fieldValue = input.getStringByField(field);
			groupValueList.add(fieldValue);
 		}
		String groupValueKey=StringUtils.join(groupValueList, ",");
		Integer count = countMap.get(groupValueKey);
		if(count!=null){
			count = count+1;
			countMap.put(groupValueKey, count);
		}else{
			countMap.put(groupValueKey, 1);
		}
		//����
		if(emit){
			Map<String,Integer> countDiffMap = diff(snapShotMap, countMap);
			for(String key:countDiffMap.keySet()){
				String[] keyValue= key.split(",");
				Map<String, Object> map=new ConcurrentHashMap<String, Object>();
				for(int i=0;i<groupFieldList.size();i++){
					map.put(groupFieldList.get(i), keyValue[i]);
				}
				map.put(GroupEnum.ct.name(), countDiffMap.get(key));
				map.put(GroupEnum.addtime.name(), DateUtil.getCurFullTimestamp());
				collector.emit(new Values(map));
				collector.ack(input); 
				try {
					memcachedClient.set(name, 60 * 60 * 24 * 2, countMap);
				} catch (Exception e) {
					logger.error("memcache �쳣  bolt=" + name, e);
				}
				logger.debug("[execute bolt="+name+"]emit map="+map);
			}
			copyMap(countMap,snapShotMap);
			emit=false;
		}else{
			return;
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(GroupEnum.map.name()));
	}

	/** 
	* @Method: diff 
	* @Description: newMap����key��ͬ��ֵ�������key����Ŀ
	* @param oldMap
	* @param newMap
	* @return Map<String,Integer>
	*/
	public static Map<String,Integer> diff(Map<String, Integer> oldMap,Map<String, Integer> newMap){
 		Set<String> removeSet = new HashSet();
		Map<String, Integer> retainMap = new HashMap();
		for(Map.Entry<String, Integer> newEntry:newMap.entrySet()){
			for(Map.Entry<String, Integer> oldEntry:oldMap.entrySet()){
				if(oldEntry.getKey().equals(newEntry.getKey()) &&  oldEntry.getValue()!=null && newEntry.getValue()!=null && oldEntry.getValue()>=newEntry.getValue()){
					removeSet.add(newEntry.getKey());
				}
			}
			if(!removeSet.contains(newEntry.getKey())){
				retainMap.put(newEntry.getKey(), newEntry.getValue());
			}
		}
		return retainMap;
	}
	
	/** 
	* @Method: copyMap 
	* @Description: Map����
	* @param ori
	* @param dest void
	*/
	public static void copyMap(Map<String, Integer> ori,Map<String, Integer> dest){
		if(dest!=null && ori!=null){
			for(Map.Entry<String, Integer> entry:ori.entrySet()){
				dest.put(entry.getKey(), entry.getValue());
			}
		}
	}
}
