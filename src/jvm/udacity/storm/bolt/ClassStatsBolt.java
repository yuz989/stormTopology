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
import java.util.Arrays;
import java.util.List;

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
      ArrayList<Object> parsed_llogs = (ArrayList<Object>) tuple.getValueByField("parsed_llogs");
        
      // for each chapter
      for(int i=0; i<parsed_llogs.size(); i++)
      {
          LearningLog<Object, Object> llog = (LearningLog<Object, Object>) parsed_llogs.get(i);    
          Integer page = llog.page;
          Integer questionType = llog.questionType;
          
          String key_class_stat = "rb." + tcode + ".stats";
          String key_identity_stat = "rb." + tcode + "." + identity;

          /**
            * 連連看    : 1    T or F        result            -- ok 
            * 句子排列  : 4    T or F         result            -- ok
            * 單字排列  : 5    T or F         result            -- ok
            * 克漏字    : 6    T or F         result            -- ok   
            * 單擇      : 2    num_answer    answer             -- ok
            * 是非      : 3    T or F        answer             -- ok
            * 理解力    : 7    T or F         result, answer
            * 複選      : 8                   result, answer
          **/
          if( questionType <= 5 ) 
          {

            ArrayList<Object> answer;
            if( questionType == 2 || questionType == 3 )
              answer = llog.answer;
            else
              answer = llog.result; 
            
            for(int j=0; j<answer.size(); j++)
            { 
              String page_offset = String.valueOf( page + j );

              String ans = String.valueOf( (Integer) answer.get(j) );  
              String old_ans = redis.hget(key_identity_stat, page_offset);
          
              if(old_ans==null)  
              {
                if( (Integer) answer.get(j) != 0)
                {
                  redis.hincrby(key_class_stat, page_offset + "." + ans, (long) 1);
                  redis.hset(key_identity_stat, page_offset, ans);
                }
              }
              else if(ans != old_ans)
              {
                redis.hincrby(key_class_stat, page_offset + "." + old_ans, (long) -1);
                if( (Integer) answer.get(j) != 0)
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

          }  


          else if( questionType == 6 )
          {
            
            ArrayList<Object> result = llog.result;
          
            for(int j=0; j<result.size(); j++)
            { 

              String page_offset = String.valueOf( page + j );          

              ArrayList<Integer> ans = (ArrayList<Integer>) result.get(j);
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

          }

          else if( questionType == 7 )
          {
             

          }
          else if( questionType == 8 )
          {
            System.out.println("\n\n\n\n\n\n\n multi select stats \n\n\n\n\n\n\n");
          
            for(int j=0; j<llog.result.size(); j++)
            {
              String page_offset = String.valueOf( page + j );
              String new_result = String.valueOf( (Integer) llog.result.get(j) );
              String old_result = redis.hget( key_identity_stat, page_offset + ".R" );

              // slow !!
              ArrayList<Integer> ansSubArray = (ArrayList<Integer>) llog.answer.get(j);
              if(old_result == null)  // answer does not exist
              { 
                if(new_result != "0")
                {
                  redis.hset(key_identity_stat, page_offset + ".R", new_result);
                  redis.hincrby(key_class_stat, page_offset + ".R." + new_result, (long) 1);                  

                  String storedString = "";                  
                  for(int k=0; k<ansSubArray.size(); k++)
                  {
                    storedString += String.valueOf(ansSubArray.get(k)) + ";";
                    redis.hincrby(key_class_stat, page_offset + "." + String.valueOf(ansSubArray.get(k)), (long) 1);
                  }
                  redis.hset(key_identity_stat, page_offset, storedString);
                }
              }
              else if(old_result != new_result || (new_result == old_result && new_result != "1") )
              {                 
                if( old_result != new_result )
                {                  
                  redis.hset(key_identity_stat, page_offset + ".R", new_result);                
                  redis.hincrby(key_class_stat, page_offset + ".R." + old_result, (long) -1);                  
                  redis.hincrby(key_class_stat, page_offset + ".R." + new_result, (long) 1);                  
                }

                String str_old_ans = redis.hget(key_identity_stat, page_offset);                
                List<String> split_old_ans = new ArrayList<String>(Arrays.asList(str_old_ans.split(";")));                                   

                // decrement old
                for(int k=0; k<split_old_ans.size(); k++)
                {
                  redis.hincrby(key_class_stat, page_offset + "." + split_old_ans.get(k), (long) -1);                  
                }

                // increment new 
                String storedString = "";                  
                for(int k=0; k<ansSubArray.size(); k++)
                {
                  storedString += String.valueOf(ansSubArray.get(k)) + ";";
                  redis.hincrby(key_class_stat, page_offset + "." + String.valueOf(ansSubArray.get(k)), (long) 1);
                }
                redis.hset(key_identity_stat, page_offset, storedString);

              }              

            }
            
          }


          else
          {
            continue;
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