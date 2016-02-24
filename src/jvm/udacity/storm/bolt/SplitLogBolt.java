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

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import udacity.storm.LearningLog;

public class SplitLogBolt extends BaseRichBolt
{
    OutputCollector _collector;
  
    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
      String tcode = tuple.getStringByField("tcode");
      String identity = tuple.getStringByField("identity");
      String llog_string = tuple.getStringByField("learning_log");   

      try
      {
          JSONParser parser = new JSONParser();
          Object obj = parser.parse(llog_string);
          JSONObject jobj = (JSONObject) obj;
          JSONArray answerDataArray = (JSONArray) jobj.get("answerData"); 

          ArrayList<LearningLog> llogs = new ArrayList<LearningLog>(); 
          for(int i=0; i<answerDataArray.size(); i++)
          { 
            JSONObject answerData = (JSONObject) answerDataArray.get(i);                         

            Integer page         = ((Long) answerData.get("page")).intValue();
            Integer questionType = ((Long) answerData.get("questionType")).intValue(); 
            JSONArray answerJArray = (JSONArray) answerData.get("answer");  // 用戶上傳作答
            JSONArray resultJArray = (JSONArray) answerData.get("result");  // 對錯
            
            Object answer;  
            LearningLog llog = new LearningLog();

            // 連連看, 是非, 單字排列, 句子排列 --> 只統計對錯
            if( questionType == 1 || questionType == 3 || questionType == 4 || questionType == 5 )  
            {
              answer =  new ArrayList<Integer>();
              for(int j=0; j<answerJArray.size(); j++)
                ((ArrayList<Integer>) answer).add( ((Long)resultJArray.get(j)).intValue() );
            }
            else if( questionType == 2 && answerData.get("isMultiple") == null ) //單選題  --> 統計答案
            {
              answer =  new ArrayList<Integer>();
              for(int j=0; j<answerJArray.size(); j++)
                ((ArrayList<Integer>) answer).add( ((Long)answerData.get(j)).intValue() );
            }
            else if(questionType == 6) // 克漏字 --> 統計對錯            
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
            else  // 複選 & 理解力測驗 --> 統計答案 & 對錯
            {
              continue;
              /*if(answerData.get("isMultiple") != null)
              {
                  questionType = 8; // multiple selection
              }*/           
            }

            llog.page = page;
            llog.questionType = questionType;
            llog.answer = answer; 
            llogs.add(llog);            
          }

          /*System.out.println("@@@@@@@@\n\n\n\n\n\n" );
          for(int x=0; x< llogs.size(); ++x)
          {
            System.out.println("page:" + String.valueOf(llogs.get(x).page) + " qtype:" + String.valueOf(llogs.get(x).questionType) ); 
            ArrayList<Integer> ans = (ArrayList<Integer>) llogs.get(x).answer;
            for(int y=0; y< ans.size(); ++y)
              System.out.print( String.valueOf( ans.get(y) ) + ", " );
            System.out.print("\n");
          }
          System.out.println("\n\n\n\n\n\n@@@@@@@@" );*/

          _collector.emit( new Values(tcode, identity, llogs) );
      }
      catch(Exception exp) 
      { 
          System.out.println(exp.toString() + "\n\n\n\n\n\n\n");
      }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare( new Fields("tcode", "identity", "parsed_llogs") );
    }
}