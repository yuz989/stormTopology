package udacity.storm.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.ArrayList;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import udacity.storm.LearningLog;

public class ClassStatsBolt extends BaseRichBolt
{
  OutputCollector _collector;
  RedisConnection<String,String> redis;

  private String connectionString;
  public ClassStatsBolt(String connectionString)
  {
      this.connectionString = connectionString;
  }

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         collector)
  {
    _collector = collector;
    RedisClient client = new RedisClient(connectionString,6379);
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
    try
    {
      String tcode = tuple.getStringByField("tcode");
      String identity = tuple.getStringByField("identity");
      ArrayList<LearningLog> parsed_llogs = (ArrayList<LearningLog>) tuple.getValueByField("parsed_llogs");

          /*System.out.println("IdentityStatsBolt@@@@@@@@\n\n\n\n" );
          for(int x=0; x< parsed_llogs.size(); ++x)
          {
            System.out.println("page:" + String.valueOf(parsed_llogs.get(x).page) + " qtype:" + String.valueOf(parsed_llogs.get(x).questionType) ); 
            ArrayList<Integer> ans = (ArrayList<Integer>) parsed_llogs.get(x).answer;
            for(int y=0; y< ans.size(); ++y)
              System.out.print( String.valueOf( ans.get(y) ) + ", " );
            System.out.print("\n");
          }
          System.out.println("\n\n\n\n@@@@@@@@IdentityStatsBolt" );*/

      // for each chapter
      for(int i=0; i<parsed_llogs.size(); i++)
      {
        LearningLog llog = parsed_llogs.get(i);    
        Integer page = llog.page;
        Integer questionType = llog.questionType;
        ArrayList<Object> answer = (ArrayList<Object>) llog.answer;
        
        String key_class_stat = "rb." + tcode + ".stats";
        String key_identity_stat = "rb." + tcode + "." + identity;

        // for each question
        for(int j=0;j<answer.size();j++){ 

            String page_offset = String.valueOf( page + j ); 

            if( questionType <= 5 ) // 其它題型                   
            {
              String ans = String.valueOf( (Integer) answer.get(j));  
              String old_ans = redis.hget(key_identity_stat, page_offset);
            
              if(old_ans==null)  
              {
                if(answer.get(j) != 0)
                {
                  redis.hincrby(key_class_stat, page_offset + "." + ans, (long) 1);
                  redis.hset(key_identity_stat, page_offset, ans);
                }
              }
              else if(ans != old_ans)
              {
                redis.hincrby(key_class_stat, page_offset + "." + old_ans, (long) -1);
                if(answer.get(j) != 0)
                {
                  redis.hset(key_identity_stat, page_offset, ans);
                  redis.hincrby(key_class_stat, page_offset + "." + ans, (long) 1);
                }
                else
                {
                  redis.hdel(key_identity_stat, page_offset);
                }
              }                 
            }
            else // 克漏字, 理解力, 複選
            {
              ArrayList<Integer> ans = (ArrayList<Integer>) answer.get(j);

              if(questionType == 6) // 克漏字
              {
                String old_ans = redis.hget(key_identity_stat, page_offset);
                String new_ans;
                
                Boolean torf = true;
                Boolean empty = true;  
                for(int k=0; k<ans.size(); k++)
                {
                  if(ans.get(k) != 1)
                    torf = false;
                  if(ans.get(k) != 0)
                    empty = false; 
                }
                new_ans = String.valueOf( ( torf ) ? 1 : ( (empty) ? 0 : 2) );

                if(old_ans==null)  
                {
                  if(new_ans != "0")
                  {
                    redis.hset(key_identity_stat, page_offset, new_ans);
                    redis.hincrby(key_class_stat, page_offset + "." + new_ans, (long) 1);
                  }
                }
                else if(new_ans != old_ans)
                {
                  redis.hincrby(key_class_stat, page_offset + "." + old_ans, (long) -1);
                  if(new_ans != "0")
                  {
                    redis.hincrby(key_class_stat, page_offset + "." + new_ans, (long) 1);
                    redis.hset(key_identity_stat, page_offset, new_ans);
                  }
                  else
                  {
                    redis.hdel(key_identity_stat, page_offset);
                  }
                }               
              
              }
              else if(questionType == 7) // 理解力測驗
              {
                answer =  new ArrayList<ArrayList<Integer>>();
                for(int j=0; j<answerJArray.size(); j++)
                {
                  ( (ArrayList<ArrayList<Integer>>) answer).add( new ArrayList<Integer>() );                              
                  JSONArray subAnsJarray = (JSONArray) resultJArray.get(j);
                  for(int k=0; k<subAnsJarray.size(); k++)
                    ( (ArrayList<ArrayList<Integer>>) answer).get(j).add( ((Long)subAnsJarray.get(k)).intValue() );
                }
              }
              else if(questionType == 8) //複選題
              {
                continue;
              }
              else
              {
                continue; // skip
              }
            }     
        }
      }
    }
    catch(Exception exp)
    {
      System.out.print("\n\n\n\n[ERR]ClassStatsBolt:" + exp.toString() + "\n");
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    return;
  }
}