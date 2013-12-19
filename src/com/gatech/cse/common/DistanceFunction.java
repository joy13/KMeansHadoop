package com.gatech.cse.common;

public class DistanceFunction {

	public double getCategoricalDistance(MovieWritable movie1, MovieWritable movie2) {
		// TODO Auto-generated method stub
		double distance = 0;
		double genreDistance;
		double castDistance;
		double langDistance;
		double countryDistance;
		double keywordDistance;
		double certificateDistance;
		
		if(movie1.getDirector().compareTo(movie2.getDirector()) != 0)
		{
			distance = distance + CommonConstants.DIRECTOR_WEIGHT;
		}
		if(movie1.getWriter().compareTo(movie2.getWriter()) != 0)
		{
			distance = distance + CommonConstants.WRITER_WEIGHT;
		}		
		if(movie1.getColor().compareTo(movie2.getColor()) != 0)
		{
			distance = distance + CommonConstants.COLOR_WEIGHT;
		}
		if(movie1.getProducer().compareTo(movie2.getProducer()) != 0)
		{
			distance = distance + CommonConstants.PRODUCER_WEIGHT;
		}
		
		genreDistance = getCustomDistance(movie1.getGenre(),movie2.getGenre());
		castDistance = getCustomDistance(movie1.getCast(),movie2.getCast());
		langDistance = getCustomDistance(movie1.getLanguage(),movie2.getLanguage());
		countryDistance = getCustomDistance(movie1.getCountry(),movie2.getCountry());
		keywordDistance = getCustomDistance(movie1.getKeywords(),movie2.getKeywords());
		certificateDistance = getCustomDistance(movie1.getCertificate(),movie2.getCertificate());
		distance = distance + CommonConstants.GENRE_WEIGHT*genreDistance + 
				CommonConstants.CAST_WEIGHT*castDistance + CommonConstants.LANGUAGE_WEIGHT*langDistance 
				+ CommonConstants.COUNTRY_WEIGHT*countryDistance + 
				CommonConstants.KEYWORD_WEIGHT*keywordDistance + 
				CommonConstants.CERTIFICATE_WEIGHT*certificateDistance;
		return distance;
	}

	private double getCustomDistance(String value1, String value2) {
		// TODO Auto-generated method stub
		int distance = 0;
		String values1[] = value1.split(",");
		String values2[] = value2.split(",");
		int max = 0;
		int count = 0;
		boolean isvalue1max = false;
		if(values1.length > values2.length) {
			max = values1.length;
			count = values2.length;
			isvalue1max = true;
		}
		else
		{
			max = values2.length;
			count = values1.length;
		}
		int intersect = max;
		if(isvalue1max)
		{
			for (int i = 0; i<max; i++)
			{
				for(int j = 0; j<count; j++)
				{
					if(values1[i].equalsIgnoreCase(values2[j]))
						intersect--;
				}
			}
		}
		else
		{
			for (int i = 0; i<max; i++)
			{
				for(int j = 0; j<count; j++)
				{
					if(values1[j].equalsIgnoreCase(values2[i]))
						intersect--;
				}
			}
		}
		distance = intersect/max;
		return distance;
	}
	
	public double getNumericDistance(MovieWritable movie1, MovieWritable movie2)
	{
		double distance = 0;
		double releaseDist = 300 - Math.abs(movie1.getRelease() - movie2.getRelease())/300;
		double ratingDist = 10 - Math.abs(movie1.getRating() - movie2.getRating())/10;
		double voteDist = (Math.max(movie1.getVote(), movie2.getVote()) - Math.abs(movie1.getVote() -
				movie2.getVote()))/Math.max(movie1.getVote(), movie2.getVote());
		double runtimeDist = (Math.max(movie1.getRuntime(), movie2.getRuntime()) 
				- Math.abs(movie1.getRuntime() - movie2.getRuntime()))/Math.max(movie1.getRuntime(),
						movie2.getRuntime());
		distance = releaseDist + ratingDist + voteDist + runtimeDist;
		return distance;		
	}
	
	public double getDistance(MovieWritable movie1, MovieWritable movie2)
	{
		double distance = getNumericDistance(movie1, movie2) + getCategoricalDistance(movie1, movie2);
		return distance;		
	}


}
