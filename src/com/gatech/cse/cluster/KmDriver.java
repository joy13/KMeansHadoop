package com.gatech.cse.cluster;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.gatech.cse.common.MovieWritable;
import com.gatech.cse.common.SequenceFileWriter;


public class KmDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		final Log LOG = LogFactory.getLog(KmDriver.class);
		int iteration = 1;
	    Configuration conf = new Configuration();
	    conf.set("num.iteration", iteration + "");
	    LOG.debug("initializing paths");
	    FileSystem fs = FileSystem.getLocal(conf);
	    
	    Path in = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/input/data.seq");
	    Path center = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/center/cen.seq");
	    conf.set("centroid.path", center.toString());
	    Path out = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/output/depth_1");
	    
	    //added for PACE cluster
	    //Path in = new Path("~/Data/input/data.seq");
	    //Path center = new Path("~/Data/center/cen.seq");
	    //conf.set("centroid.path", center.toString());
	    //Path out = new Path("~/Data/output/depth_1");
	    
	    LOG.debug("initializing paths DONE");
	    
	    if (fs.exists(out)) {
		      fs.delete(out, true);
		    }

		    if (fs.exists(center)) {
		      fs.delete(out, true);
		    }

		    if (fs.exists(in)) {
		      fs.delete(in, true);
		    }
		fs.setVerifyChecksum(false);
		    
		long start = System.currentTimeMillis();;   
	    
	    Job job = new Job(conf);
	    job.setJobName("KMeans Clustering");

	    job.setMapperClass(KmMapper.class);
	    job.setReducerClass(KmReducer.class);
	    job.setJarByClass(KmMapper.class);
	    FileInputFormat.addInputPath(job, in);
	    
	    LOG.debug("Going to write seq file");

	    //Maintaining this sequence is improtant, as clustercenters are written from datapoints
	    SequenceFileWriter.writeMovieData(conf, in, fs);
	    SequenceFileWriter.writeMovieCenters(conf, center, fs);

	    FileOutputFormat.setOutputPath(job, out);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
	    job.setOutputKeyClass(MovieWritable.class);
	    job.setOutputValueClass(MovieWritable.class);

	    job.waitForCompletion(true);

	    long counter = job.getCounters()
	        .findCounter(KmReducer.Counter.CONVERGED).getValue();
	    iteration++;
	    while (counter > 0) {
	      System.out.println("STARTING ITERATION NO: " +iteration);
	      conf = new Configuration();
	      conf.set("centroid.path", center.toString());
	      conf.set("num.iteration", iteration + "");
	      job = new Job(conf);
	      job.setJobName("KMeans Clustering " + iteration);

	      job.setMapperClass(KmMapper.class);
	      job.setReducerClass(KmReducer.class);
	      job.setJarByClass(KmMapper.class);

	      in = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/output/depth_" + (iteration - 1) + "/");
	      out = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/output/depth_" + iteration);
	      
	      //for PACE cluster
	      //in = new Path("~/Data/output/depth_" + (iteration - 1) + "/");
	      //out = new Path("~/Data/output/depth_" + iteration);

	      FileInputFormat.addInputPath(job, in);
	      if (fs.exists(out))
	        fs.delete(out, true);

	      FileOutputFormat.setOutputPath(job, out);
	      job.setInputFormatClass(SequenceFileInputFormat.class);
	      job.setOutputFormatClass(SequenceFileOutputFormat.class);
	      job.setOutputKeyClass(MovieWritable.class);
	      job.setOutputValueClass(MovieWritable.class);
	      job.waitForCompletion(true);
	      System.out.println("ENDING ITERATION NO: " +iteration);
	      iteration++;
	      counter = job.getCounters().findCounter(KmReducer.Counter.CONVERGED)
	          .getValue();
	    }
	    
	    long end = System.currentTimeMillis();
	    
	    System.out.println("TOTAL RUNTIME: "+(end - start) + " ms");
	    
	    if(counter == 0)
	    {
	    	System.out.println("CONVERGED!");
	    }
	    
	    Path result = new Path("/Users/udaymbp2009/Documents/workspace/ClusterTest/output/depth_" + (iteration - 1) + "/");
	    
	    //added for PACE CLUSTER
	    //Path result = new Path("~Data/output/depth_" + (iteration - 1) + "/");

	    FileStatus[] stati = fs.listStatus(result);
	    for (FileStatus status : stati) {
	      if (!status.isDir()) {
	        Path path = status.getPath();
	        if (!path.getName().equals("_SUCCESS")) {
	          LOG.info("FOUND " + path.toString());
	          try {
	        	  BufferedWriter writetxt = new BufferedWriter(new FileWriter("~/Data/file.txt"));
	        	  //added for PACE cluster
	        	  //BufferedWriter writetxt = new BufferedWriter(new FileWriter("/Users/udaymbp2009/Desktop/CSE/Project/Result/file.txt"));
	        	  writetxt.newLine();
	        	  writetxt.newLine();
	        	  SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
	    	              conf);
	            MovieWritable center1 = new MovieWritable();
	            MovieWritable value = new MovieWritable();
	            int count = 0;
	            while (reader.next(center1, value)) {
	              //LOG.info(key + " / " + v);
	            	writetxt.write(center1.getId());
	            	writetxt.newLine();
	            	writetxt.write(value.getTitle());
	            	writetxt.newLine();
	            }
	            writetxt.close();
	            reader.close();
	          }
	          catch(Exception ex)
	            {
	            	System.out.println("exception in driver: reading sequence file");
	            }
	        }
	      }
	    }
	  }

	}

