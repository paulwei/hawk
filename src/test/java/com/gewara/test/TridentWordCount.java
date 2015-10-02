/**
 * 
 */
package com.gewara.test;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class TridentWordCount {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, Exception {
		Config config=new Config();
		config.setMaxSpoutPending(20);
		
		if(args.length==0){
			config.setMaxTaskParallelism(5);
			
			LocalCluster localCluster=new LocalCluster();
			LocalDRPC localDRPC=new LocalDRPC();

			StormTopology stormTopology = buildTopology(localDRPC);
			
			localCluster.submitTopology("wordcount", config, stormTopology);
//			System.out.println("DRPC RESULT:"+localDRPC.execute("words", "cat the dog jumped"));

			
			for(int i=0;i<100;i++){
				System.out.println("DRPC RESULT:"+localDRPC.execute("words", "cat the dog jumped"));
				Thread.sleep(1000);
			}
		}else{
			config.setNumWorkers(3);

			StormTopology stormTopology = buildTopology(null);
			
			StormSubmitter.submitTopology(args[0], config, stormTopology);
		}
	}
	
	public static StormTopology buildTopology(LocalDRPC localDRPC){
		FixedBatchSpout spout=new FixedBatchSpout(new Fields("sentence"), 10, 
//				new Values("The cow jumped over the moon"),
//                new Values("The man went to the store and bought some candy"),
//                new Values("Four score and seven years ago"),
//                new Values("How many apples can you eat"),
                new Values("wei hong lin")
		);
		spout.setCycle(true);
		TridentTopology topology=new TridentTopology();
		TridentState tridentState = topology.newStream("spout1", spout)
			.parallelismHint(16)
			.each(new Fields("sentence"), new Split(), new Fields("item"))
			.each(new Fields("item"), new LowerCase(), new Fields("word"))
			//输入item  lowcase 操作   输出word
			.groupBy(new Fields("word"))//根据上一步输出word聚合
			//MemoryMapState装在内存中
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
//			.persistentAggregate(MemcachedState.transactional(serverLocations), new Count(), new Fields("count"))        
//           MemcachedState.transactional()
			//聚合并且持久化
			.parallelismHint(6);
		
		//调用方localDRPC.execute("words", "cat the dog jumped"))
		//words为输入流名字要一致，args固定的域名代表后面参数,每个batch喷发带固定参数   
		topology.newDRPCStream("words", localDRPC)
			.each(new Fields("args"), new Split(), new Fields("word"))
			.groupBy(new Fields("word"))
			//由于DRPC stream是使用跟TridentState完全同样的group方式（按照“word”字段进行group），每个单词的查询会被路由到TridentState对象管理和更新这个单词的分区去执行。
			.stateQuery(tridentState, new Fields("word"), new MapGet(), new Fields("count"))
			//tridentState的输出结果作为输入源
			.each(new Fields("count"), new FilterNull())
			.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		return topology.build();
	}
	/*
	Trident在如何最大程度的保证执行topogloy性能方面是非常智能的。在topology中会自动的发生两件非常有意思的事情：
	读取和更新状态的操作 (比如说 persistentAggregate 和 stateQuery) 会自动的是batch的形式操作状态。 如果有20次更新需要被同步到存储中，Trident会自动的把这些操作汇总到一起，只做一次读一次写，而不是进行20次读20次写的操作。因此你可以在很方便的执行计算的同时，保证了非常好的性能。
	Trident的聚合器已经是被优化的非常好了的。Trident并不是简单的把一个group中所有的tuples都发送到同一个机器上面进行聚合，而是在发送之前已经进行过一次部分的聚合。打个比方，Count聚合器会先在每个partition上面进行count，然后把每个分片count汇总到一起就得到了最终的count。这个技术其实就跟MapReduce里面的combiner是一个思想。
	*/
}


class Split extends BaseFunction{

	/* (non-Javadoc)
	 * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence=tuple.getString(0);
		for(String word:sentence.split(" ")){
			collector.emit(new Values(word));
//			System.out.println("word:"+word);
		}
	}
	
}

class LowerCase extends BaseFunction{

	/* (non-Javadoc)
	 * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String word=tuple.getString(0);
		String lowerCase = word.toLowerCase();
		collector.emit(new Values(lowerCase));
	}
	
}