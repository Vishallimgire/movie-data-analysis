# GCP Cloud data pipeline for Movie data analysis 
**Project Overview**

This is movie data analysis batch pipeline project has done following analysis
  
  1.  Show the aggregated number of ratings per year
  2. Show the average monthly number of ratings
  3. Show the rating levels distribution
  4. Show the 18 movies that are tagged but not rated
  5. Show the movies that have rating but no tag
  6. Focusing on the rated untagged movies with more than 30 user ratings,
    show the top 10 movies in terms of average rating and number of
    ratings
  7. What is the average number of tags per movie in tagsDF? And the
      average number of tags per user? How does it compare with the
    average number of tags a user assigns to a movie?
  8. Identify the users that tagged movies without rating them
  9. What is the average number of ratings per user in ratings DF? And the
    average number of ratings per movie?

**Technology stack**
1. Python
2. Pyspark
3. Google Cloud Platform
4. Airflow


**Architecture**

![image](https://github.com/Vishallimgire/movie-data-analysis/assets/17686705/65f86e18-2d1f-4c12-a8eb-e537282c0ac6)


**Steps for project**
1. Add data set to input  bucket of GCP.
2. Create bucket for output and archive data.
3. Create cloud composer for Airflow and add airflow_spark_job.py script in dag folder.
4. Add script in script bucket.
5. Add necessary configuration of GCP cloud.
6. Then after trigger script on apache airflow then check live changes happening on GCP cloud. 
