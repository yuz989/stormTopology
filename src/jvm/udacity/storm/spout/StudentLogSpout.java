package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

public class StudentLogSpout extends BaseRichSpout {
  
  SpoutOutputCollector _collector;
  RedisConnection<String,String> redis;
  String connectionString = "52.74.196.202";

  public StudentLogSpout(String connectionString)
  {
      this.connectionString = connectionString;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    RedisClient client = new RedisClient(connectionString,6379);
    redis = client.connect();
  }

  @Override
  public void nextTuple() {
    
    String key = "rb.student.log";
    String log = redis.lpop(key);
    
    if(log == null)
      return;

    try
    {
        String[] arr = log.split("@");   
        String tcode    = arr[0];
        String identity = arr[1];
        String learning_log = arr[2];      
         _collector.emit(new Values(tcode, identity, learning_log));
    }
    catch(Exception exp)
    {
      return; 
    }
 
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare( new Fields("tcode", "identity", "learning_log") );
  }

}
