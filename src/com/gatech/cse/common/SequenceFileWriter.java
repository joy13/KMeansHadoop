package com.gatech.cse.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileWriter {
	static ArrayList<MovieWritable> movielist = null;
	public static void writeMovieData(Configuration conf, Path in,
			FileSystem fs) throws IOException {
		try {
			fs.setVerifyChecksum(false);
			SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf,
					in, MovieWritable.class, MovieWritable.class);
			MovieWritable dummy = new MovieWritable(0,"d",0,0,0,0,"d","d","d","d","d","d","d","d","d","d");
			movielist = ImdbParser.parse(CommonConstants.FILE_NAME);
			if(movielist != null || movielist.size() == 0)
			{
				for(int i = 0; i< movielist.size(); i++)
				{
					dataWriter.append(dummy,new MovieWritable(movielist.get(i)));
				}
				dataWriter.close();	
			}
			else
			{
				System.out.println("MOVIE LIST IS NULL!! ABORT!!!");
			}
		}
		catch(Exception ex)
		{
			System.out.println("exception in writing seqfile: ");
			ex.printStackTrace();
		}
	}

	public static void writeMovieCenters(Configuration conf, Path center,
			FileSystem fs) throws IOException {			
		try {
			SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf,
					center, MovieWritable.class, IntWritable.class);
			final IntWritable value = new IntWritable(0);
			if(movielist == null || movielist.size() == 0)
			{
				System.out.println("Writing centers: MOVIE LIST IS NULL!! ABORT!!!");
			}
			for(int i = 0; i<CommonConstants.K_VALUE; i++) {				
				int index = randInt(1, (movielist.size()-1));
				centerWriter.append(new MovieWritable(movielist.get(index)),value);
			}
			centerWriter.close();
		}
		catch(Exception ex) {
			System.out.println("exception in writing seqfile: Centers");
			ex.printStackTrace();
		}
	}
	
	public static int randInt(int min, int max) {
	    Random rand = new Random();
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
}
