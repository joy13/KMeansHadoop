package com.gatech.cse.cluster;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import com.gatech.cse.common.DistanceFunction;
import com.gatech.cse.common.MovieWritable;

public class KmReducer extends Reducer<MovieWritable, MovieWritable, MovieWritable, MovieWritable>{
	  public static enum Counter {
	    CONVERGED
	  }
	  private final List<MovieWritable> centers = new ArrayList<MovieWritable>();

	  @Override
	  protected void reduce(MovieWritable key, Iterable<MovieWritable> values,
	      Context context) throws IOException, InterruptedException {

	    List<MovieWritable> movieList = new ArrayList<MovieWritable>();
	    DistanceFunction distanceFunc = new DistanceFunction();
	    MovieWritable newCenter = null;
	    for (MovieWritable value : values) {
	      movieList.add(new MovieWritable(value));
	      
	      if(newCenter == null)
	      {
	    	  newCenter = new MovieWritable();
	    	  newCenter.setRating(value.getRating());
	    	  newCenter.setRelease(value.getRelease());
	    	  newCenter.setRuntime(value.getRuntime());
	    	  newCenter.setVote(value.getVote());
	      }
	      else
	      {
	    	  newCenter.setRating(newCenter.getRating() + value.getRating());
	    	  newCenter.setRelease(newCenter.getRelease() + value.getRelease());
	    	  newCenter.setRuntime(newCenter.getRuntime() + value.getRuntime());
	    	  newCenter.setVote(newCenter.getVote() + value.getVote());  
	      }
	    }
	    newCenter.setRating(newCenter.getRating()/movieList.size());
	    newCenter.setRelease(newCenter.getRelease()/movieList.size());
	    newCenter.setRuntime(newCenter.getRuntime()/movieList.size());
	    newCenter.setVote(newCenter.getVote()/movieList.size());
	    
	    double minCategoricalDist = 999999.0;
	    int id = 0;
	    for (int i = 0; i< movieList.size(); i++) {
	    	double categoricalDist = 0;
	    	for(int j = 0; j< movieList.size(); j++) {
	    		categoricalDist = categoricalDist + distanceFunc.getCategoricalDistance(movieList.get(j), movieList.get(i));	    		
	    	}
	    	if(categoricalDist < minCategoricalDist)
	    	{ 
	    		minCategoricalDist = categoricalDist;
	    		newCenter.setId(movieList.get(i).getId());
	    		newCenter.setTitle(movieList.get(i).getTitle());
	    		newCenter.setCast(movieList.get(i).getCast());
	    		newCenter.setDirector(movieList.get(i).getDirector());
	    		newCenter.setWriter(movieList.get(i).getWriter());
	    		newCenter.setCertificate(movieList.get(i).getCertificate());
	    		newCenter.setCountry(movieList.get(i).getCountry());
	    		newCenter.setColor(movieList.get(i).getColor());
	    		newCenter.setGenre(movieList.get(i).getGenre());
	    		newCenter.setKeywords(movieList.get(i).getKeywords());
	    		newCenter.setLanguage(movieList.get(i).getLanguage());
	    		newCenter.setProducer(movieList.get(i).getProducer());
	    	}
	    }
	    
	    MovieWritable center = new MovieWritable(newCenter);
	    centers.add(center);
	    for (MovieWritable movie : movieList) {
	      context.write(center, movie);
	    }
	    if (center.compareTo(key) != 0)
	    {
	      context.getCounter(Counter.CONVERGED).increment(1);
	    }
	  }
	  
	  @Override
	  protected void cleanup(Context context) throws IOException,
	      InterruptedException {
	    super.cleanup(context);
	    Configuration conf = context.getConfiguration();
	    Path outPath = new Path(conf.get("centroid.path"));
	    FileSystem fs = FileSystem.get(conf);
	    fs.setVerifyChecksum(false);
	    fs.delete(outPath, true);
	    try {
	    		SequenceFile.Writer out = SequenceFile.createWriter(fs,
	    	        context.getConfiguration(), outPath, MovieWritable.class,
	    	        IntWritable.class);
	    	    final IntWritable value = new IntWritable(0);
	    		for (MovieWritable center : centers) {
	    			out.append(center,value);
	    		}
	    		out.close();
	    	}
	    catch (Exception ex)
	    {
	    	System.out.println("exception in reducer: cleanup");
	    	ex.printStackTrace();
	    }
	  }
}
