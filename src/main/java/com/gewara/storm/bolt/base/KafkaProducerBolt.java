/**  
* @Project: hawk
* @Title: KafkaProducerBolt.java
* @Package com.gewara.storm.bolt
* @Description: 实时计算结果推送kafka
* @author honglin.wei@gewara.com
* @date Mar 18, 2014 11:26:13 AM
* @version V1.0  
*/

package com.gewara.storm.bolt.base;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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
import com.gewara.util.DateUtil;
import com.gewara.util.JsonUtils;
 

public class KafkaProducerBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerBolt.class); 
	private static final long serialVersionUID = 1L;
	private transient OutputCollector collector;
	private transient Producer<String, String> producer ;
	private transient ProducerConfig config ;
	private transient Properties props ;
	
	private String tablename;

	public KafkaProducerBolt(String tablename) {
		this.tablename = tablename;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.props = new Properties();
		props.put("metadata.broker.list", ConfigFactory.getConfigProps().getString(ConfigProps.KEY_BROKER_KAFKA));
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
        this.config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
	}

	@Override
	public void execute(Tuple input) {
    	Map map = (Map)input.getValue(0);
		map.put(ConfigProps.TABLE_NAME, tablename);
		String json = JsonUtils.writeMapToJson(map);
	    String warpValue=json; 
	    KeyedMessage<String, String> data = new KeyedMessage<String, String>(ConfigProps.TOPIC_TABLE,tablename+DateUtil.timeMillis(), warpValue);
        producer.send(data);
	    collector.emit(new Values(data));
        collector.ack(input);
      logger.debug("produce data:"+data);
	}

	 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
