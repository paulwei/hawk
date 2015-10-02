/**  
* @Project: hawk
* @Title: ProjectBolt.java
* @Package com.gewara.storm.bolt
* @Description: ÏîÄ¿Á÷Bolt
* @author honglin.wei@gewara.com
* @date Mar 18, 2014 3:53:45 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.base;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.gewara.constant.GroupEnum;
 
public class FieldByBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Fields fields;
 	
	public FieldByBolt(Fields fields) {
		this.fields = fields;
	}


	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

 
	@Override
	public void execute(Tuple input) {
		Map map = (Map)input.getValue(0);
		Values values  = new Values();
		for(String field:fields.toList()){
			String fieldValue = (String)map.get(field);
			values.add(fieldValue);
		}
		values.add(map);
		collector.emit(values);
		collector.ack(input);
	}

	 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> list = fields.toList();
		list.add(GroupEnum.map.name());
		declarer.declare(new Fields(list));
	}

}
