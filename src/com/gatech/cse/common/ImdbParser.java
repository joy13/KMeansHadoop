package com.gatech.cse.common;
import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ImdbParser {
	public static int getRuntime(String s){
		Matcher matcher = Pattern.compile("\\d+").matcher(s);
		matcher.find();
		return Integer.valueOf(matcher.group());
	}

	public static MovieWritable getMovie(ArrayList<String> tempMovieFeatureList){
		int id = Integer.parseInt(tempMovieFeatureList.get(0));
		String title = tempMovieFeatureList.get(1);
		int release = Integer.parseInt(tempMovieFeatureList.get(2));
		double rating = Double.parseDouble(tempMovieFeatureList.get(3));;
		int vote = Integer.parseInt(tempMovieFeatureList.get(4));

		// runtime requires some cleaning of data
		int runtime = getRuntime(tempMovieFeatureList.get(5));

		String director = tempMovieFeatureList.get(6);
		String writer = tempMovieFeatureList.get(7);
		String genre = tempMovieFeatureList.get(8);
		String keywords = tempMovieFeatureList.get(9);
		String cast = tempMovieFeatureList.get(10);
		String country = tempMovieFeatureList.get(11);
		String language = tempMovieFeatureList.get(12);
		String color = tempMovieFeatureList.get(13);
		String producer = tempMovieFeatureList.get(14);
		String certificate = tempMovieFeatureList.get(15);
		return new MovieWritable(id, title, release, rating, vote, runtime, director, writer, genre, keywords, cast, country, language, color, producer, certificate);
	}

	public static ArrayList<MovieWritable>  parse(String filename){

		ArrayList<String> tempMovieFeatureList = new ArrayList<String>();
		ArrayList<MovieWritable> movies = new ArrayList<MovieWritable>();

		try{
			System.out.println("Working Directory = " + System.getProperty("user.dir"));
			FileInputStream fstream = new FileInputStream(filename);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;

			while ((strLine = br.readLine()) != null)   {

				// End of a movie.
				if(strLine.isEmpty()){
					movies.add(getMovie(tempMovieFeatureList));
					//System.out.println(tempMovieFeatureList.get(5));
					tempMovieFeatureList = new ArrayList<String>();
				}
				// Parse the features of the movie
				else{
					String yo = strLine;
					tempMovieFeatureList.add(yo);
				}
			}
			in.close();

			// Add last movie
			movies.add(getMovie(tempMovieFeatureList));


		}catch (Exception e){
			System.err.println("Error: " + e.getMessage() + e.getClass());
			e.printStackTrace();
			System.exit(0);
		}

		return movies;
	}

}
