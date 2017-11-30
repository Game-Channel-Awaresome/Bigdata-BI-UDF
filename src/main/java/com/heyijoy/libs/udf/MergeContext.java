//package com.heyijoy.libs.udf;
//
//import java.text.ParseException;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
//import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
//
//public class MergeContext extends AbstractGenericUDAFResolver {
//	static final Log LOG = LogFactory.getLog(MergeContext.class);
//
//  	public static class State {
//	    Map<String, String> context; 
//	    String xwhen;
//	  }
//	  
//	public static class Evaluator implements UDAFEvaluator {
//	    
//	    private State state;
//	    
//	    public Evaluator() {
//	    	super();
//	    	state = new State();
//	    	init();
//	    }
//	    
//	    public void init() {    
//	      state.context = new HashMap<String, String>();
//	      state.xwhen = "0000-00-00 00:00:00";
//	    }
//	  
//	    public boolean iterate(String xwhen, Map<String, String> context) throws ParseException {
//	        try {
//	    	    int is_newer_record = this.state.xwhen.compareTo(xwhen);
//	    	    for(String exits_key: context.keySet()){
//	    		    if(this.state.context.containsKey(exits_key)){
//		    		    if(is_newer_record < 0){
//		    			    this.state.context.put(exits_key, context.get(exits_key));
//		    		    }
//	    		    } else {
//	    			    this.state.context.put(exits_key, context.get(exits_key));
//	    		    }
//	    	    }
//	    	  
//	    	    if(is_newer_record < 0){
//	    		  this.state.xwhen = xwhen;
//	    	    }
//	    	  
//		    } catch (Exception e) {
////			  	  e.printStackTrace();
//		    }
//	        return true;
//	    }
//
//	    public State terminatePartial() {
//	        return state.context.size() == 0 ? null : state;
//	    }
//
//	    public boolean merge(State o) {
//	      if (o != null) {
//	         String xwhen = o.xwhen;
//	         Map<String, String> context = o.context;
//	         
//	         int is_newer_than_others = this.state.xwhen.compareTo(xwhen);
//	         for(String exits_key: context.keySet()){
//	        	 if(this.state.context.containsKey(exits_key)){
//		    		  if(is_newer_than_others < 0){
//		    			  this.state.context.put(exits_key, context.get(exits_key));
//		    		  }
//	        	 } else {
//	        		 this.state.context.put(exits_key, context.get(exits_key));
//	        	 }
//	         }
//	         
//	         if (is_newer_than_others < 0) {
//	        	 this.state.xwhen = xwhen;
//	         }
//	         
//	      }      
//	      return true;
//	    }
//	  
//	    public State terminate() {
//	    	return this.state.context.size() == 0 ? null : this.state;
//	    }
//	  }
//
//}