/**  
* @Project: hawk
* @Title: BaseFilterBolt.java
* @Package com.gewara.storm.bolt
* @Description: ¹ýÂËÆ÷Bolt
* @author honglin.wei@gewara.com
* @date Mar 19, 2014 3:03:46 PM
* @version V1.0  
*/

package com.gewara.storm.bolt.filter;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
 

public abstract class BaseFilterBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	@Override
	public void execute(Tuple input) {
		Map map = (Map)input.getValue(0);
		if(map!=null&&!map.isEmpty()&&accept(map)){
			collector.emit(new Values(map));
			collector.ack(input);
		}

	}
 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("map"));
	}
	
    /** 
    * @Method: accept 
    * @Description: ÊÇ·ñ¹ýÂË
    * @param map
    * @return boolean
    */
    public abstract boolean accept(Map map);

}
