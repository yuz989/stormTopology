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
          ArrayList<Object> llogs = new ArrayList<Object>(); 

          JSONParser parser = new JSONParser();
          Object obj = parser.parse(llog_string);
          JSONObject jobj = (JSONObject) obj;

          if(jobj.get("page") != null)
          {
            System.out.print("single page submit!!!\n\n\n\n\n\n\n");

            Integer questionType = ((Long) jobj.get("questionType")).intValue(); 

            if(questionType == 1 || questionType == 4 || questionType == 5)
            {
              LearningLog<Integer, Integer> llog = new LearningLog<Integer, Integer>();              

              llog.questionType = ((Long) jobj.get("questionType")).intValue(); 
              llog.page = ((Long) jobj.get("page")).intValue();
              llog.result =  new ArrayList<Integer>();
              llog.result.add( ((Long) jobj.get("result")).intValue() );

              llogs.add(llog);
            }
            else if( questionType == 3 || (questionType == 2 && (jobj.get("isMultiple") != null && ((Boolean) jobj.get("isMultiple")) == false )) )
            {
              LearningLog<Integer, Integer> llog = new LearningLog<Integer, Integer>();              
              
              llog.questionType = ((Long) jobj.get("questionType")).intValue(); 
              llog.page = ((Long) jobj.get("page")).intValue();
              llog.answer =  new ArrayList<Integer>();
              llog.answer.add( ((Long) jobj.get("answer")).intValue() );

              llogs.add(llog);
            }
            else if(questionType == 2)  // 複選            
            {
              LearningLog<ArrayList<Integer>, Integer> llog = new LearningLog<ArrayList<Integer>, Integer>();
              llog.questionType = 8; 
              llog.page = ((Long) jobj.get("page")).intValue();

              llog.result = new ArrayList<Integer>();
              llog.result.add( ((Long) jobj.get("result")).intValue() ); 

              llog.answer = new ArrayList<ArrayList<Integer>>();
              llog.answer.add(new ArrayList<Integer>());
              try
              {
                JSONArray answerJArray = (JSONArray) jobj.get("answer");  // 對錯  
             
                for(int i=0; i<answerJArray.size(); i++)
                {
                  llog.answer.get(0).add( ((Long) answerJArray.get(i)).intValue()  );    
                }  
              } 
              catch(Exception exp)
              {
                llog.answer.get(0).add( ((Long) jobj.get("answer")).intValue() );
              }             
              llogs.add(llog);
            }
            else if(questionType == 6)
            {
              LearningLog<Integer, ArrayList<Integer>> llog = new LearningLog<Integer, ArrayList<Integer>>();
              llog.questionType = ((Long) jobj.get("questionType")).intValue(); 
              llog.page = ((Long) jobj.get("page")).intValue();
              llog.result = new ArrayList<ArrayList<Integer>>();
              llog.result.add(new ArrayList<Integer>());

              JSONArray resultJArray = (JSONArray) jobj.get("result");  
              for(int i=0; i<resultJArray.size(); i++)
              {                                             
                llog.result.get(0).add( ((Long)resultJArray.get(i)).intValue() );
              }
              llogs.add(llog);
            }
            else if(questionType == 7)
            {
              LearningLog<ArrayList<Integer>, ArrayList<Integer>> llog = new LearningLog<ArrayList<Integer>, ArrayList<Integer>>();
              llog.questionType = ((Long) jobj.get("questionType")).intValue(); 
              llog.page = ((Long) jobj.get("page")).intValue();
            }
 
          }    
          else
          {
            System.out.print("multiple pages submit!!!\n\n\n\n\n\n\n");
            JSONArray answerDataArray = (JSONArray) jobj.get("answerData"); 

            for(int i=0; i<answerDataArray.size(); i++)
            { 
              JSONObject answerData = (JSONObject) answerDataArray.get(i);                         

              Integer page         = ((Long) answerData.get("page")).intValue();
              Integer questionType = ((Long) answerData.get("questionType")).intValue(); 
              JSONArray answerJArray = (JSONArray) answerData.get("answer");  // 用戶上傳作答
              JSONArray resultJArray = (JSONArray) answerData.get("result");  // 對錯
                                  
              if( questionType == 1 || questionType == 4 || questionType == 5 )  
              {
                LearningLog<Integer, Integer> llog = new LearningLog<Integer, Integer>();
                llog.questionType = questionType;
                llog.page = page;

                llog.result =  new ArrayList<Integer>();
                for(int j=0; j<resultJArray.size(); j++)
                  llog.result.add( ((Long)resultJArray.get(j)).intValue() );
                
                llogs.add(llog);
              }             
              else if( questionType == 3 || (questionType == 2 && (answerData.get("isMultiple") != null && ((Boolean) answerData.get("isMultiple")) == false )) ) //單選題  
              {
                LearningLog<Integer, Integer> llog = new LearningLog<Integer, Integer>();
                llog.questionType = questionType;
                llog.page = page;

                llog.answer =  new ArrayList<Integer>();
                for(int j=0; j<answerJArray.size(); j++)
                  llog.answer.add( ((Long)answerJArray.get(j)).intValue() );
                
                llogs.add(llog);
              }
              else if( questionType == 2 )   //複選
              {
                LearningLog<ArrayList<Integer>, Integer> llog = new LearningLog<ArrayList<Integer>, Integer>();
                llog.questionType = 8;
                llog.page = page;          

                llog.answer = new ArrayList<ArrayList<Integer>>();
                llog.result = new ArrayList<Integer>();
                for(int j=0; j<resultJArray.size(); j++)
                {
                  llog.result.add( ((Long)resultJArray.get(j)).intValue() );                  
                  llog.answer.add( new ArrayList<Integer>() );
                  try
                  {
                    JSONArray ansSubArray = (JSONArray) answerJArray.get(j);  
                    for(int k=0; k<ansSubArray.size(); k++)
                    {
                      llog.answer.get(j).add( ((Long) ansSubArray.get(k)).intValue() );    
                    }  
                  } 
                  catch(Exception exp)
                  {
                    llog.answer.get(j).add( ((Long) answerJArray.get(j)).intValue() );
                  }                    
                }

                llogs.add(llog);
              }     
              else if( questionType == 6 ) // 克漏字 --> 統計對錯            
              { 
                LearningLog<Integer, ArrayList<Integer>> llog = new LearningLog<Integer, ArrayList<Integer>>();
                llog.questionType = questionType;
                llog.page = page;

                llog.result = new ArrayList<ArrayList<Integer>>();
                for(int j=0; j<resultJArray.size(); j++)
                {
                  llog.result.add( new ArrayList<Integer>() );                              
                  JSONArray resultItemJArray = (JSONArray) resultJArray.get(j);
                  for(int k=0; k<resultItemJArray.size(); k++)
                    llog.result.get(j).add( ((Long)resultItemJArray.get(k)).intValue() );
                }

                llogs.add(llog);
              }           
              else if( questionType == 7 )
              {
                LearningLog<ArrayList<Integer>, ArrayList<Integer>> llog = new LearningLog<ArrayList<Integer>, ArrayList<Integer>>();
                llog.questionType = questionType;
                llog.page = page;

                llog.answer = new ArrayList<ArrayList<Integer>>();
                llog.result = new ArrayList<ArrayList<Integer>>();

                for(int j=0; j<resultJArray.size(); j++)
                {
                  llog.answer.add( new ArrayList<Integer>() );                              
                  llog.result.add( new ArrayList<Integer>() );                 
                 
                  JSONArray resultItemJArray = (JSONArray) resultJArray.get(j);
                  for(int k=0; k<resultItemJArray.size(); k++)
                  {
                    llog.answer.get(j).add(0);
                    llog.result.get(j).add( ((Long)resultItemJArray.get(k)).intValue() );                
                  }

                  JSONArray answerItemJArray = (JSONArray) answerJArray.get(j);
                  for(int k=0; k<answerItemJArray.size(); k++)
                  {
                      JSONArray subAnsItemJArray = (JSONArray) answerItemJArray.get(k);    
                      for(int l=0; l<subAnsItemJArray.size(); l++)
                      {
                        if( subAnsItemJArray.get(l) == "True")
                        {
                          llog.answer.get(j).add(l+1);  
                          break;
                        }
                      }
                  }
                }

                llogs.add(llog);
              }  
      
            }
          
          }
          
          _collector.emit( new Values(tcode, identity, llogs) );
      }
      catch(Exception exp) 
      { 
          System.out.println(exp.toString() + "\n\n\n\n\n");
      }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare( new Fields("tcode", "identity", "parsed_llogs") );
    }
}