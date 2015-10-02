/**  
* @Project: hawk
* @Title: GroupDistBolt.java
* @Package com.gewara.storm.bolt
* @Description: 分组计数Bolt
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

public class GroupSumBolt<T extends Number> extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private volatile boolean emit = true;     //发射标记
	private Fields  groupField;      //分组字段
	private String sumField;         //计数字段
 	private Long period;             //统计间隔
	private String  name;            //bolt名称
 	private transient MemcachedClient memcachedClient; //缓存

    private static final Logger logger = LoggerFactory.getLogger(GroupSumBolt.class); 
	private Map<String, T> sumMap=new ConcurrentHashMap<String, T>();
	private Map<String, T> snapShotMap=new ConcurrentHashMap<String,T>();

	public GroupSumBolt(Fields groupField,String sumField,String name) {
		this(groupField,sumField,name,null);
	}
	public GroupSumBolt(Fields groupField,String sumField,String name,Long period) {
		this.groupField = groupField;
		this.sumField = sumField;
		this.name = name;
		this.period = period!=null?period:(60*1000);
  	}
	protected void init(){
		Date delayClear = DateUtil.getBeginningTimeOfDay(DateUtil.addDay(DateUtil.getCurDate(), 1));
		Date delayStat = DateUtil.getCurFullTimestamp();
		Timer clearTimer = new Timer();
		clearTimer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				sumMap.clear();
				snapShotMap.clear();
				try {
					memcachedClient.set(name, 60 * 60 * 24 * 2, sumMap);
				} catch (Exception e) {
					logger.error("memcache 异常  bolt=" + name, e);
				}
				logger.info("clear memcache bolt="+name+",sumMap.size="+sumMap.size()+",snapShotMap.size"+snapShotMap.size());
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
			Map<String, T> m = (Map<String, T>)memcachedClient.get(name);
			if(m!=null){copyMap(m,sumMap);}
			logger.info("[initData bolt="+name+"]memMap="+m+",sumMap="+sumMap);
		} catch (Exception e) {
			logger.error("memcache 异常  bolt="+name, e);
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
		Map tmp = (Map)input.getValueByField(GroupEnum.map.name());
	    String strVal = tmp.get(sumField)!=null?tmp.get(sumField).toString().trim():null;
	    Number curVal =  StringUtils.isNumeric(strVal)&&StringUtils.isNotBlank(strVal)?new Long(strVal):0;
		Number sum = sumMap.get(groupValueKey);
		if(sum!=null){
			sum  = sum.longValue()+curVal.longValue();
			sumMap.put(groupValueKey,(T)sum);
		}else{
			sumMap.put(groupValueKey,(T)curVal);
		}
		//快照
		if(emit){
			Map<String,T> countDiffMap = diff(snapShotMap, sumMap);
			for(String key:countDiffMap.keySet()){
				String[] keyValue= key.split(",");
				Map<String, Object> map=new ConcurrentHashMap<String, Object>();
				for(int i=0;i<groupFieldList.size();i++){
					map.put(groupFieldList.get(i), keyValue[i]);
				}
				map.put(GroupEnum.sm.name(), countDiffMap.get(key));
				map.put(GroupEnum.addtime.name(), DateUtil.getCurFullTimestamp());
				collector.emit(new Values(map));
				collector.ack(input); 
				try {
					memcachedClient.set(name, 60 * 60 * 24 * 2, sumMap);
				} catch (Exception e) {
					logger.error("memcache 异常  bolt=" + name, e);
				}
				logger.debug("[execute bolt="+name+"]emit map="+map);
			}
			copyMap(sumMap,snapShotMap);
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
	* @Description: newMap保留key相同数值大或者新key的条目
	* @param oldMap
	* @param newMap
	* @return Map<String,Number>
	*/
	public Map<String,T> diff(Map<String, T> oldMap,Map<String, T> newMap){
 		Set<String> removeSet = new HashSet();
		Map<String, T> retainMap = new HashMap();
		for(Map.Entry<String, T> newEntry:newMap.entrySet()){
			for(Map.Entry<String, T> oldEntry:oldMap.entrySet()){
				if(oldEntry.getKey().equals(newEntry.getKey()) &&  oldEntry.getValue()!=null && newEntry.getValue()!=null && oldEntry.getValue().longValue()>=newEntry.getValue().longValue()){
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
	* @Description: Map拷贝
	* @param ori
	* @param dest void
	*/
	public  void copyMap(Map<String,T> ori,Map<String, T> dest){
		if(dest!=null && ori!=null){
			for(Map.Entry<String, T> entry:ori.entrySet()){
				dest.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
}
