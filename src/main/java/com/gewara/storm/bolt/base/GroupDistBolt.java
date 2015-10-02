/**  
* @Project: hawk
* @Title: GroupDistBolt.java
* @Package com.gewara.storm.bolt
* @Description: 分组去重Bolt
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
import com.gewara.util.BloomFilter;
import com.gewara.util.DateUtil;

public class GroupDistBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private volatile boolean emit = true;     //发射标记
	private Fields  groupField;       //分组字段
	private String distField;        //去重字段
	private Long   period;           //统计间隔
	private String name;             //bolt名称
 	private transient MemcachedClient memcachedClient; //缓存
 	
    private static final Logger logger = LoggerFactory.getLogger(GroupDistBolt.class); 
	private Map<String, Integer> counter=new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> snapShotMap=new ConcurrentHashMap<String, Integer>();
	private transient Map<String, BloomFilter> countMap;

	public GroupDistBolt(Fields groupField,String distField,String name) {
		this(groupField,distField,name,null);
	}
 
	public GroupDistBolt(Fields groupField,String distField,String name,Long period) {
		this.distField = distField;
		this.groupField = groupField;
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
				countMap.clear();
				counter.clear();
				snapShotMap.clear();
				try {
					memcachedClient.set(name, 60 * 60 * 24 * 2, countMap);
				} catch (Exception e) {
					logger.error("memcache 异常  bolt=" + name, e);
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
			String host =ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
			Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
			memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
			Map<String, BloomFilter> m = (Map<String, BloomFilter>)memcachedClient.get(name);
			String msg="memMap={";
			if(m!=null){
				copy2MapBloom(m,countMap);
				for(String key:m.keySet()){
					BloomFilter value = m.get(key);
					msg+="["+key+"="+value.getSize()+"]";
				}
			}
			logger.info("[initData bolt="+name+"]memMap="+msg+"}");
		} catch (Exception e) {
			logger.error("memcache 异常  bolt="+name, e);
 			e.printStackTrace();
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.countMap = new ConcurrentHashMap<String, BloomFilter>();
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
		List<String> groupValueList = new ArrayList();
		for(String field:groupFieldList){
			String fieldValue = input.getStringByField(field);
			groupValueList.add(fieldValue);
		}
		String groupValueKey=StringUtils.join(groupValueList, ",");
		Map tmp = (Map)input.getValueByField(GroupEnum.map.name());
		String distValue = tmp.get(distField)!=null?tmp.get(distField).toString().trim():"";
		if(StringUtils.isBlank(distValue)){return;}
		if(countMap.get(groupValueKey)!=null){
			countMap.get(groupValueKey).add(distValue);
		}else{
			BloomFilter filter = new BloomFilter();
			filter.add(distValue);
			countMap.put(groupValueKey, filter);
		}
		copy2Map(countMap,counter);
		//快照
		if(emit){//前一分钟数据需要后一分钟数据触发
			Map<String,Integer> countDiffMap = diff(snapShotMap, counter);
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
					logger.error("memcache 异常  bolt=" + name, e);
				}
				logger.debug("[execute bolt="+name+"]emit map="+map);
			}
			copyMap(counter,snapShotMap);
			emit=false;
		}else{
			return;
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(GroupEnum.map.name()));
	}
 
	public String getDistField() {
		return distField;
	}
	public void setDistField(String distField) {
		this.distField = distField;
	}
	public long getPeriod() {
		return period;
	}
	public void setPeriod(long period) {
		this.period = period;
	}
	
	/** 
	* @Method: diff 
	* @Description: newMap保留key相同数值大或者新key的条目
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
	* @Description: Map拷贝
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
	
	public static void copy2MapBloom(Map<String, BloomFilter> ori,Map<String, BloomFilter> dest){
		if(dest!=null && ori!=null){
			for(Map.Entry<String, BloomFilter> entry:ori.entrySet()){
				dest.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public static void copy2Map(Map<String, BloomFilter> ori,Map<String, Integer> dest){
		if(dest!=null && ori!=null){
			for(Map.Entry<String, BloomFilter> entry:ori.entrySet()){
				dest.put(entry.getKey(), entry.getValue().getSize());
			}
		}
	}

	

}
