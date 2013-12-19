package com.gatech.cse.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.gatech.cse.common.DistanceFunction;
import com.gatech.cse.common.MovieWritable;

public class KmMapper extends Mapper<MovieWritable, MovieWritable, MovieWritable, MovieWritable> {
	
	private final List<MovieWritable> centers = new ArrayList<MovieWritable>();
	private DistanceFunction distanceFunc;

	@Override
	  protected void setup(Context context) throws IOException,
	      InterruptedException {
	    super.setup(context);
	    Configuration conf = context.getConfiguration();
	    Path centroids = new Path(conf.get("centroid.path"));
	    FileSystem fs = FileSystem.get(conf);

	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids,conf);
	    try{
	      MovieWritable key = new MovieWritable();
	      IntWritable value = new IntWritable();
	      int index = 0;
	      while (reader.next(key, value)) {
	    	  MovieWritable clusterCenter = new MovieWritable(key);
	        clusterCenter.setClusterIndex(index++);
	        centers.add(clusterCenter);
	      }
	    }
	    finally
	    {
	    	reader.close();
	    }
	    distanceFunc = new DistanceFunction();
	  }
	
	public void map(MovieWritable key, MovieWritable value, Context context) throws IOException, InterruptedException {
		MovieWritable nearest = null;
	    double nearestDistance = 99999;
	    for (MovieWritable center : centers) {
	      double distance = distanceFunc.getDistance(center,value);
	      if (nearest == null) {
	        nearest = new MovieWritable(center);
	        nearestDistance = distance;
	      } else {
	        if (nearestDistance > distance) {
	          nearest = center;
	          nearestDistance = distance;
	        }
	      }
	    }
	    context.write(nearest, value);
	}

}
