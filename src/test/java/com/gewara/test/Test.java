/**  
* @Project: hawk
* @Title: Test.java
* @Package com.gewara.test
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Mar 21, 2014 11:13:21 AM
* @version V1.0  
*/

package com.gewara.test;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import kafka.consumer.ConsumerConfig;

import org.apache.commons.lang.StringUtils;

import com.gewara.constant.UVEnum;
import com.gewara.util.DateUtil;

 

public class Test {
	private Map<String, Integer> counter=new ConcurrentHashMap<String, Integer>();

	private volatile boolean clear = false;    //清空缓存标记
    Map map  = new HashMap();
    private static final Runtime s_runtime = Runtime.getRuntime ();
    volatile boolean emit;
	/*Timer emitTimer;
	public void time(){
		Timer emitTimer = new Timer();
		emitTimer.schedule(new TimerTask(){
			@Override
			public void run() {
				emit = Boolean.TRUE;
			}
		}, 60*1000, 60*1000);
	}
	
	public void f(){
		while(true){
		if(emit){
			emit= Boolean.FALSE;
			System.out.println("emit:"+DateUtil.getCurFullTimestampStr());
		}}
	}*/
	public static Map<String,Long> diff(Map<String, Long> oldMap,Map<String, Long> newMap){
 		Set<String> removeSet = new HashSet();
		Map<String, Long> retainMap = new HashMap();
		for(Map.Entry<String, Long> newEntry:newMap.entrySet()){
			for(Map.Entry<String, Long> oldEntry:oldMap.entrySet()){
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
	public static void main(String[] args) throws Exception {
		Test test  =new Test();
		test.counter.put("aaa", 1000);
		test.counter.put("bbb", 2000);
		test.counter.clear();
		System.out.println(test.counter.isEmpty());
/*		Test  test =new Test();
		System.out.println(test.emit);
		Map sumMap = new HashMap();
		sumMap.put("aaa", new Long(9000));
		Map snapShotMap = new HashMap();
		snapShotMap.put("aaa", new Long(1000));
		snapShotMap.put("bbb", new Long(1000));
		Map<String,Long> countDiffMap = diff(snapShotMap, sumMap);
		for(String key:countDiffMap.keySet()){
			System.out.println(key+":"+countDiffMap.get(key));
		}*/
	/*	Date delayClear = DateUtil.getBeginningTimeOfDay(DateUtil.addDay(DateUtil.getCurDate(), 1));
        System.out.println(DateUtil.formatTimestamp(delayClear));
         Test test  = new Test();
         test.init();
         test.testMap();
         System.out.println("xxx:"+test.map.size());

//         test.map.clear();
         for(int i=0;i<100;i++){
         if(test.clear){
        	 test.map.clear();
        	 System.out.println("clear map");
        	 test.clear=false;
         }
         Thread.sleep(3*60*1000);
         }
//         Thread.sleep(3*60*1000);
         System.out.println("size2:"+test.map.size());
         */
		
/*		String host =ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
		Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
		MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress(host,port));
		Map<String, BloomFilter> m = (Map<String, BloomFilter>)memcachedClient.get("uv");
		String msg="memMap={";
		if(m!=null){
			for(String key:m.keySet()){
				BloomFilter value = m.get(key);
				msg+="["+key+"="+value.getSize()+"]";
			}
		}
		msg+="}";
		System.out.println(msg);*/
		
/*		String zooKeeper = "192.168.8.101:2181,192.168.8.102:2181,192.168.8.103:2181/kafka";
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zooKeeper, "group"));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("table", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("table");
		for (final KafkaStream stream : streams) {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println("message:"+new String(it.next().message()));
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
		}*/
		
	/* Map tmp = new HashMap();
	 tmp.put("uid", "1000");
		String distValue = tmp.get("uid")!=null?tmp.get("uid").toString().trim():"";
     System.out.println(distValue);
     tmp.put("tc", " 44  ");
     String strVal = tmp.get("tc")!=null?tmp.get("tc").toString().trim():null;
     Number curVal =  StringUtils.isNumeric(strVal)&&StringUtils.isNotBlank(strVal)?new Long(strVal):0;
     System.out.println(curVal);
*/
//		StringUtils.isNumeric(tmp.get("tc"))
 	/*	String host = ConfigFactory.getConfigProps().getString(ConfigProps.KEY_MEMCACHE_HOST);
		Integer port = ConfigFactory.getConfigProps().getInteger(ConfigProps.KEY_MEMCACHE_PORT);
		System.out.println("host="+host+",port"+port);
		System.out.println("NVL".equalsIgnoreCase("nvl"));
		Map map = new HashMap();
		map.put("fpkey", "");
//		map.put("cpkey", null);
		NotNvlFilterBolt nb = new NotNvlFilterBolt("cpkey");
		System.out.println(nb.accept(map));*/
	/*	 List list = new ArrayList();
		 for(int i=0;i<Integer.MAX_VALUE;i++){
			 list.add(new Integer(8000));
			 Thread.sleep(100);
		 }*/

//		long heap1=0;
//		long heap2=0;
//		boolean flag = true;
//		if(flag){
//			runGC ();
//			BloomFilter filter = new BloomFilter();
//			heap1 = usedMemory();
//		}
//		runGC ();
//		heap2 = usedMemory();
//		System.out.print(heap2-heap1);
//		BloomFilter filter = new BloomFilter();
 		
// 	   Random r = new Random();
//       System.out.println(r.nextInt(2));
		
//        String ref="http://www.soso.com/q?utf-8=ie&pid=s.idx&cid=s.idx.se&unc=&query=http%3A%2F%2Fwww.gewara.com%2Fsubject%2Fprice5.xhtml%3Fsid%3D112093905&w=&sut=811&sst0=1396578352785&lkt=1%2C1396578352096%2C1396578352096";
////        String regex = "^\\s*http://"+StringUtils.join(ConfigProps.SEARCH_ENGINEE.keySet(), "|^\\s*http://");
//        Pattern p = Pattern.compile(ConfigProps.REGEX_SEO, Pattern.CASE_INSENSITIVE);
////      Pattern p = Pattern.compile("www.sogou.com|^\\s*http://www.soso.com|^\\s*http://www.so.com|^\\s*http://cn.bing.com|^\\s*http://www.google.com.hk|^\\s*http://r.search.yahoo.com|^\\s*http://www.baidu.com", Pattern.CASE_INSENSITIVE);
//		Matcher m = p.matcher(ref==null?"":ref);
//		System.out.println(m.find());
//		
//		String ref="www.gewara.com/cinema/59980148?utm_source=baidu&utm_medium=zjcpc&utm_term=%E5%BE%B7%E6%B8%85%E9%93%B6%E9%83%BD%E6%97%B6%E4%BB%A3%E7%94%B5%E5%BD%B1%E5%A4%A7%E4%B8%96%E7%95%8C%E8%B4%AD%E7%A5%A8&utm_campaign=zhejiang";
//	    Pattern p = Pattern.compile(ConfigProps.REGEX_SEM, Pattern.CASE_INSENSITIVE);
//		Matcher m = p.matcher(ref==null?"":ref);
//		System.out.println(m.find());
//		
//		String ref="www.gewara.com/cinema/59980148?utm_source=baidu&utm_medium=zjcpc&utm_term=%E5%BE%B7%E6%B8%85%E9%93%B6%E9%83%BD%E6%97%B6%E4%BB%A3%E7%94%B5%E5%BD%B1%E5%A4%A7%E4%B8%96%E7%95%8C%E8%B4%AD%E7%A5%A8&utm_campaign=zhejiang";
//	    Pattern p = Pattern.compile(ConfigProps.REGEX_SEM, Pattern.CASE_INSENSITIVE);
//		Matcher m = p.matcher(ref==null?"":ref);
//		System.out.println(m.find());
		
//		String ref="http://www.gewara.com/activity/10462867";
//		Pattern p = Pattern.compile(ConfigProps.REGEX_DIRECT, Pattern.CASE_INSENSITIVE);
//		Matcher m = p.matcher(ref==null?"":ref);
//		System.out.println(m.find());
		
		
/*		   
		   Set<String> oldkey = new HashSet();
			Set<String> newkey = new HashSet();
			oldkey.add("111");
			oldkey.add("222");
			oldkey.add("555");
			newkey.add("111");
			newkey.add("333");
			newkey.add("555");
			
			newkey.retainAll(oldkey);
            System.out.println(newkey);*/
//     Map<String,Integer> m = BeanUtil.diff(oldmap, newmap);
		   
		  /* for(String key:m.keySet()){
			   System.out.println(key+":"+m.get(key));
		   }*/
		    
		// TODO Auto-generated method stub
//		Fields fields  =new Fields("aaa","bbb","ccc");
//		List<String> list = fields.toList();
//		list.add("map");
//		System.out.println(new Fields(list).toString());
//		Map map  = new HashMap();
//		String url = (String)map.get("url");
//		Pattern p = Pattern.compile("baidu.com", Pattern.CASE_INSENSITIVE);
//		Matcher m = p.matcher(null);
//		System.out.println(m.find());
	/*	Date d = DateUtil.getCurFullTimestamp();
		Test test = new Test();
		test.time();
     	test.f();*/
/*		Timer t = new Timer();
        t.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				System.out.println("定时bolt:"+DateUtil.getCurFullTimestampStr());
			}
		}, d, 60*1000);*/
   
	}
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}
	
	private static long usedMemory ()
    {
        return s_runtime.totalMemory () - s_runtime.freeMemory ();
    }
	 private static void runGC () throws Exception
	    {
	        // It helps to call Runtime.gc()
	        // using several method calls:
	        for (int r = 0; r < 4; ++ r) _runGC ();
	    }
	    private static void _runGC () throws Exception
	    {
	        long usedMem1 = usedMemory (), usedMem2 = Long.MAX_VALUE;
	        for (int i = 0; (usedMem1 < usedMem2) && (i < 500); ++ i)
	        {
	            s_runtime.runFinalization ();
	            s_runtime.gc ();
	            Thread.currentThread ().yield ();
	            
	            usedMem2 = usedMem1;
	            usedMem1 = usedMemory ();
	        }
	    }

	public static boolean accept(Map map) {
		Map<UVEnum,String> quryFilter = new HashMap<UVEnum,String>();
		quryFilter.put(UVEnum.sn, "101000000001");
		quryFilter.put(UVEnum.pkey, "111");
		quryFilter.put(UVEnum.uagent, "IOS");
		boolean flag = true;
        if(map!=null && quryFilter!=null){
        	for(UVEnum uv:quryFilter.keySet()){
        		 if(StringUtils.equalsIgnoreCase(quryFilter.get(uv), map.get(uv.name())!=null?map.get(uv.name()).toString():"")){
        			 flag = flag && Boolean.TRUE;
        		 }else{
        			 flag = flag && Boolean.FALSE;
        		 }
        	}
        }
	return flag;
	}
	protected void init(){
// 		Date delayClear = DateUtil.getBeginningTimeOfDay(DateUtil.getCurDate());
 		Date delayClear = DateUtil.parseTimestamp("2014-04-20 14:05:00", "yyyy-MM-dd HH:mm:ss");
 		 
		Timer clearTimer = new Timer();
		clearTimer.scheduleAtFixedRate(new TimerTask(){
			@Override
			public void run() {
				clear=true;
				System.out.println(" interval clear "+DateUtil.getCurFullTimestampStr()+",clear="+clear);
			}
		}, delayClear, 3*60*1000);
	}
	protected void  testMap(){
		this.map.put("uv",3000 );
		this.map.put("pv",5000 );
	}
}
