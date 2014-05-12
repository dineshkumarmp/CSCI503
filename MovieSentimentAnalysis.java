import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class MovieSentimentAnalysis {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        
    	public static double positiveWeight = 0;
    	public static double negativeWeight = 0;
    	
        public class doMatch extends Thread {
        	
        	String[] reviewWords;
        	String type;
        	HashMap<String, Double> weightMap = new HashMap<String, Double>();
    		HashMap<String, String> distanceMap = new HashMap<String, String>();
    		
            doMatch(String type, String[] words, HashMap<String, Double> weightMap, HashMap<String, String> distanceMap) {
            	this.type = type;
            	this.reviewWords = words;
                this.weightMap = weightMap;
                this.distanceMap = distanceMap;
            }

            public void run() {
            	double weightage=0;
            	for (String reviewWord:reviewWords)
            		{
                			for(String word:weightMap.keySet())
                    		{
                    			if (StringUtils.getLevenshteinDistance(reviewWord, word) < Integer.parseInt(distanceMap.get(word))){
                    				weightage+=weightMap.get(word);
                    				break;
                    			}
                    		}
                	}
            	if(type.equalsIgnoreCase("positive")){
            		positiveWeight = weightage;
            	}
            	else{
            		negativeWeight = weightage;
            	}
            }
        }
    	
    	
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        
        	String[] rows = value.toString().split("\t");
        	String movieName = rows[1];
        	String movieReview = rows[2];
        	double weightage=0;
        	String[] words = movieReview.split(" ");
        	
        	
        	Path pt=new Path("positive.txt");
    		FileSystem fs = FileSystem.get(new Configuration());
    		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    		HashMap<String, Double> myMapPos = new HashMap<String, Double>();
    		HashMap<String, String>positiveWords = new HashMap<String, String>();
    		try {
    			  String line;
    			  while ((line = br.readLine()) != null) {
    				   // process the line.
    					String[] rows1 = line.split(";");
    					myMapPos.put(rows1[0],Double.parseDouble(rows1[1]));
    					positiveWords.put(rows1[0],rows1[2]);			
    			  }
    		}finally{
    			br.close();
    		}

    		
    		pt=new Path("negative.txt");
    		fs = FileSystem.get(new Configuration());
    		br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    		HashMap<String, Double> myMapNeg = new HashMap<String, Double>();
    		HashMap<String, String>negativeWords = new HashMap<String, String>();
    		try {
    			  String line;
    			  while ((line = br.readLine()) != null) {
    				   // process the line.
    					String[] rows1 = line.split(";");
    					myMapNeg.put(rows1[0],Double.parseDouble(rows1[1]));		
    					negativeWords.put(rows1[0],rows1[2]);	
    			  }
    		}finally{
    			br.close();
    		}

    	
	    	doMatch positiveThread = new doMatch("positive",words,myMapPos,positiveWords);
	    	positiveThread.start();
	    	
	    	doMatch negativeThread = new doMatch("negative",words,myMapNeg,negativeWords);
	    	negativeThread.start();
	    	
	    	try {
				positiveThread.join();
				negativeThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    	
	    	
	    	weightage = positiveWeight - negativeWeight;
    	
        	Text outKey = new Text(movieName);
        	DoubleWritable outValue = new DoubleWritable(weightage);
           	output.collect(outKey, outValue);

        }
}


    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

           double sum = 0;

        	while (values.hasNext())
        	{	
        		       sum += values.next().get();
        	}
       
            output.collect(key, new DoubleWritable(sum));

        }
			
}
    

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(MovieSentimentAnalysis.class);

        conf.setJobName("MovieSentimentAnalysis");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class);
		MultipleInputs.addInputPath(conf, new Path(args[2]), TextInputFormat.class);
		MultipleInputs.addInputPath(conf, new Path(args[3]), TextInputFormat.class);
		MultipleInputs.addInputPath(conf, new Path(args[4]), TextInputFormat.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[5]));
        JobClient.runJob(conf);

    }

}



