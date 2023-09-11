# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from datetime import datetime

# COMMAND ----------

def process_data():
    bucket_path = f'gs://batch-job-bucket'
    input_path = f'{bucket_path}/input'
    output = f'{bucket_path}/output'
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Spark movie data") \
        .enableHiveSupport() \
        .getOrCreate()

    # COMMAND ----------

    # load all data set
    movie_csv = f"{input_path}/movies.csv"
    rating_csv = f"{input_path}/ratings.csv"
    tag_csv = f"{input_path}/tags.csv"

    movie_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(movie_csv)
    rating_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(rating_csv)
    tag_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(tag_csv)



    # COMMAND ----------

    movie_df.show(5)
    rating_df.show(5)
    tag_df.show(5)

    # COMMAND ----------

    rating_df1 = rating_df.withColumn("date", f.to_timestamp(f.col("timestamp")))

    # COMMAND ----------

    # # a. Show the aggregated number of ratings per year
    # select year(date), count(rating) from rating group by year
    rating_df1.groupBy(f.year(f.col("date").alias('year'))).agg(f.count('rating').alias('rating_count')).write.csv(f'{output}/rat_per_year.csv', header=True)


    # COMMAND ----------

    # b. Show the average monthly number of ratings
    # select month(date) as month, avg(rating) from rating group by month
    rating_df1.groupBy(f.month(f.col("date").alias('month'))).agg(f.avg('rating').alias('rating_avg')).write.csv(f'{output}/month_rat.csv', header=True)

    # COMMAND ----------

    # c. Show the rating levels distribution
    # SELECT rating, COUNT(*) as count
    #   FROM ratings
    #   GROUP BY rating
    #   ORDER BY rating
    rating_df1.groupBy('rating').agg(f.count('rating')).write.csv(f'{output}/rat_distribution.csv', header=True)

    # COMMAND ----------

    # d. Show the 18 movies that are tagged but not rated
    # select movieanme from movies m  left join tags t on m.movieId=t.movieId
    # movie_name is null limit 18
    movie_df.join(tag_df,movie_df.movieId==tag_df.movieId, 'leftanti').limit(18).write.csv(f'{output}/top_18_no_rat.csv', header=True)

    # COMMAND ----------

    # e. Show the movies that have rating but no tag
    # select * from movie m inner join ratings r on m.movieId = r.movieId
    # left join tags t on  m.movieId = t.movieId
    # where t.tag is null
    rating_untag_mov_df = movie_df.join(rating_df,movie_df.movieId==rating_df.movieId, 'inner').join(tag_df, movie_df.movieId==tag_df.movieId, 'leftanti').drop(rating_df.movieId)
    rating_untag_mov_df.show()

    # COMMAND ----------

    # f . Focusing on the rated untagged movies with more than 30 user ratings,
    # show the top 10 movies in terms of average rating and number of
    # ratings
    # query: select * from movie m inner join ratings r on m.movieId = r.movieId
    # left join tags t on  m.movieId = t.movieId
    # where t.tag is null

    rating_untag_mov_df.groupBy('movieId').agg(f.count('userId').alias('users'),
                                                    f.avg('rating').alias('avg_rating'),
                                                    f.count('rating').alias('number_rating')).filter('users>=30').show()

    # COMMAND ----------

    # g. What is the average number of tags per movie in tagsDF? 
    #select movieId, avg(count(tag)) from tags group by movieId
    tag_df.groupBy('movieId').agg(f.count('tag').alias('avg_tag_count')).groupBy('movieId').agg(f.avg('avg_tag_count')).show()


    # COMMAND ----------

    # And the average number of tags per user? 
    # select userId, avg(count(tag)) from tags group by userId
    tag_df.groupBy('userId').agg(f.count('tag').alias('avg_tag_count')).groupBy('userId').agg(f.avg('avg_tag_count')).show()

    # COMMAND ----------

    # how does it compare with average number of tags a user assigns to a movie?
    # select userId,movieId, avg(count(tag)) from tags group by userId, movieId
    tag_df.groupBy('userId', 'movieId').agg(f.count('tag').alias('avg_tag_count')).groupBy('userId', 'movieId').agg(f.avg('avg_tag_count')).show()

    # COMMAND ----------

    # h. Identify the users that tagged movies without rating them
    tag_df.join(rating_df1, tag_df.userId==rating_df1.userId, 'leftanti').show(n=50)

    # COMMAND ----------

    # i. What is the average number of ratings per user in ratings DF? And the
    # average number of ratings per movie?
    rating_df1.groupBy('userId').agg(f.count('rating').alias('rating_count')).groupBy('userId').agg(f.avg('rating_count')).show()
    rating_df1.groupBy('movieId').agg(f.count('rating').alias('rating_count')).groupBy('movieId').agg(f.avg('rating_count')).show()



    # COMMAND ----------

    # j. What is the predominant (frequency based) genre per rating level?


    # COMMAND ----------
if __name__ == "__main__":
    process_data()


