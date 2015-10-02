/**  
* @Project: hawk
* @Title: ReadJsonBolt.java
* @Package com.gewara.storm.bolt
* @Description: Json½âÎöÆ÷Bolt
* @author honglin.wei@gewara.com
* @date Mar 18, 2014 3:31:46 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.base;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.gewara.util.JsonUtils;
 

public class ReadJsonBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	 
	@Override
	public void execute(Tuple input) {
		String json = input.getString(0);
		Map map = JsonUtils.readJsonToMap(json);
		if(map!=null&&!map.isEmpty()){
			collector.emit(new Values(map));
			collector.ack(input);
		}
	}

 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("map"));
	}

}
