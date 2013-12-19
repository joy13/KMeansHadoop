package com.gatech.cse.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieWritable implements WritableComparable<MovieWritable> {

	private int id;
	private String title;
	private int release;
	private double rating;
	private int vote;
	private int runtime;
	private String director;
	private String writer;
	private String genre;
	private String keywords;
	private String cast;
	private String country;
	private String language;
	private String color;
	private String producer;
	private String certificate;
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getRelease() {
		return release;
	}

	public void setRelease(int release) {
		this.release = release;
	}

	public double getRating() {
		return rating;
	}

	public void setRating(double rating) {
		this.rating = rating;
	}

	public int getVote() {
		return vote;
	}

	public void setVote(int vote) {
		this.vote = vote;
	}

	public String getDirector() {
		return director;
	}

	public void setDirector(String director) {
		this.director = director;
	}

	public String getWriter() {
		return writer;
	}

	public void setWriter(String writer) {
		this.writer = writer;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getCast() {
		return cast;
	}

	public void setCast(String cast) {
		this.cast = cast;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public int getRuntime() {
		return runtime;
	}

	public void setRuntime(int runtime) {
		this.runtime = runtime;
	}

	public String getCertificate() {
		return certificate;
	}

	public void setCertificate(String certificate) {
		this.certificate = certificate;
	}

	public String getProducer() {
		return producer;
	}

	public void setProducer(String producer) {
		this.producer = producer;
	}

	private int clusterIndex;
	public int getClusterIndex() {
		return clusterIndex;
	}

	public void setClusterIndex(int clusterIndex) {
		this.clusterIndex = clusterIndex;
	}

	public MovieWritable() {
		super();
	}
	
	public MovieWritable(int id, String title, int release, double rating, int vote, int runtime,
			String director, String writer, String genre, String keywords, String cast, String country, 
			String lang, String color, String producer, String certificate)
	{
		super();
		this.id = id;
		this.title = title;
		this.release = release;
		this.rating = rating;
		this.vote = vote;
		this.runtime = runtime;
		this.director = director;
		this.writer = writer;
		this.genre = genre;
		this.keywords = keywords;
		this.cast = cast;
		this.country = country;
		this.language = lang;
		this.color = color;
		this.producer = producer;
		this.certificate = certificate;
	}
	
	public MovieWritable(MovieWritable movie) {
		super();
		this.id = movie.id;
		this.title = movie.title;
		this.release = movie.release;
		this.rating = movie.rating;
		this.vote = movie.vote;
		this.runtime = movie.runtime;
		this.director = movie.director;
		this.writer = movie.writer;
		this.genre = movie.genre;
		this.keywords = movie.keywords;
		this.cast = movie.cast;
		this.country = movie.country;
		this.color = movie.color;
		this.language = movie.language;
		this.producer = movie.producer;
		this.certificate = movie.certificate;
	}
	@Override
	public void readFields(DataInput dataIn) throws IOException {
		id  = dataIn.readInt();
		
		title = dataIn.readUTF();
		release = dataIn.readInt();
		rating = dataIn.readDouble();
		vote = dataIn.readInt();
		runtime = dataIn.readInt();
		director = dataIn.readUTF();
		writer = dataIn.readUTF();
		genre = dataIn.readUTF();
		keywords = dataIn.readUTF();
		cast = dataIn.readUTF();
		country = dataIn.readUTF();
		color = dataIn.readUTF();
		language = dataIn.readUTF();
		certificate = dataIn.readUTF();
		producer = dataIn.readUTF();
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		dataOut.writeInt(id);
		dataOut.writeUTF(title);
		dataOut.writeInt(release);
		dataOut.writeDouble(rating);
		dataOut.writeInt(vote);
		dataOut.writeInt(runtime);
		dataOut.writeUTF(director);
		dataOut.writeUTF(writer);
		dataOut.writeUTF(genre);
		dataOut.writeUTF(keywords);
		dataOut.writeUTF(cast);
		dataOut.writeUTF(country);
		dataOut.writeUTF(color);
		dataOut.writeUTF(language);
		dataOut.writeUTF(certificate);
		dataOut.writeUTF(producer);
	}

	@Override
	public int compareTo(MovieWritable movie) {
		int cmp1 = release - movie.release;
	    if(cmp1 != 0) {
	      return cmp1;
	    }
	    int cmp2 = (int) (rating - movie.rating);
	    if(cmp2 != 0) {
		      return cmp2;
		    }
	    int cmp3 = vote - movie.vote;
	    if(cmp3 != 0) {
		      return cmp3;
		    }
	    int cmp4 = runtime - movie.runtime;
	    if(cmp4 != 0) {
		      return cmp4;
		    }
	    int cmp5 = director.compareTo(movie.director);
	    if(cmp5 != 0) {
		      return cmp5;
		    }
	    int cmp6 = writer.compareTo(movie.writer);
	    if(cmp6 != 0) {
		      return cmp6;
		    }
	    int cmp7 = genre.compareTo(movie.genre);
	    if(cmp7 != 0) {
		      return cmp7;
		    }
	    int cmp8 = keywords.compareTo(movie.keywords);
	    if(cmp8 != 0) {
		      return cmp8;
		    }
	    int cmp9 = cast.compareTo(movie.cast);
	    if(cmp9 != 0) {
		      return cmp9;
		    }
	    int cmp10 = country.compareTo(movie.country);
	    if(cmp10 != 0) {
		      return cmp10;
		    }
	    int cmp11 = color.compareTo(movie.color);
	    if(cmp11 != 0) {
		      return cmp11;
		    }
	    int cmp12 = producer.compareTo(movie.producer);
	    if(cmp12 != 0) {
		      return cmp12;
		    }
	    return(language.compareTo(movie.language));
		 }
}

