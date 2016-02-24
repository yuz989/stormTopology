package udacity.storm;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import udacity.storm.spout.StudentLogSpout;
import udacity.storm.bolt.SplitLogBolt;
import udacity.storm.bolt.ClassStatsBolt;

public class ClassroomReportTopology {

  public static void main(String[] args) throws Exception
  {
    String connectionString;
    Boolean mode_dbg = false;

    if (args != null && args.length > 0)
    {
        connectionString = args[0];
    }
    else
    {
        System.out.print("[Error] missing redis connection string");
        return;    
    }

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("student-log-spout", new StudentLogSpout(connectionString), 6);    
    builder.setBolt("split-log-bolt", new SplitLogBolt(), 5).shuffleGrouping("student-log-spout");
    builder.setBolt("class-stats-bolt", new ClassStatsBolt(connectionString), 5).fieldsGrouping("split-log-bolt", new Fields("identity"));

    Config conf = new Config();

    if(false) {
      System.out.print("running topology in production mode\n");
      conf.setNumWorkers(3);
      conf.put(Config.NIMBUS_HOST, args[0]);
      conf.put(Config.NIMBUS_THRIFT_PORT,6627);
      conf.setMaxSpoutPending(5000);
      StormSubmitter.submitTopology("classroom-report-topology", conf, builder.createTopology());
    }
    else 
    {
      conf.setDebug(true);
      System.out.print("running topology in local mode\n");
      conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("classroom-report-topology", conf, builder.createTopology());    
    }

  }

}
