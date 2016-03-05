package udacity.storm;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import udacity.storm.spout.StudentLogSpout;
import udacity.storm.bolt.SplitLogBolt;
import udacity.storm.bolt.ClassStatsBolt;

import udacity.storm.LearningLog;

public class ClassroomReportTopology {

  public static void main(String[] args) throws Exception
  {
    String redisHost = "52.74.196.202";
    Boolean debugMode = false;    

    if (args != null && args.length > 1)
    {
        redisHost = args[0];
        if(args.length >=2)
        {
          debugMode = args[1].equals("-dbg");
        }        
    }
    else
    {
        System.out.println("[WARNING] use default settings: redisHost:=52.74.196.202");
    }

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("student-log-spout", new StudentLogSpout(redisHost), 6);    
    builder.setBolt("split-log-bolt", new SplitLogBolt(), 5).shuffleGrouping("student-log-spout");
    builder.setBolt("class-stats-bolt", new ClassStatsBolt(redisHost), 5).fieldsGrouping("split-log-bolt", new Fields("identity"));

    Config conf = new Config();
    conf.registerSerialization(LearningLog.class);

    if(debugMode) {      
      System.out.print("running topology in local mode\n");
      conf.setDebug(true);
      conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("classroom-report-topology", conf, builder.createTopology());    
    }
    else 
    {
      System.out.print("running topology in production mode\n");
      conf.setNumWorkers(3);
      conf.put(Config.NIMBUS_HOST, "127.0.0.1");
      conf.put(Config.NIMBUS_THRIFT_PORT,6627);
      conf.setMaxSpoutPending(5000);
      StormSubmitter.submitTopology("classroom-report-topology", conf, builder.createTopology());
    }

  }

}
