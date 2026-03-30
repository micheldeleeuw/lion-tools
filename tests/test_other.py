import pytest
import pyspark.sql.functions as F

<<<<<<< HEAD
def test_transpose(spark, movies):
    movies.eTranspose(n=4).show()
    movies.eTranspose(n=4, add_data_type=True).show()
    movies.eTranspose(n=4, add_data_type=True, data_type='double').show()
    movies.eTranspose(n=4, add_data_type=True, data_type='int').show()
    movies.eTranspose(n=4, column_name_source="Title").show()
    movies.select(*[col for i, col in enumerate(movies.columns) if i < 5]).eTranspose('Director', n=4).show()

    with pytest.raises(ValueError):
        movies.eTranspose(n=4, column_name_source="non existing column").show()
=======
# def test_transpose(spark, movies):
#     movies.eTranspose(n=4).show()
#     movies.eTranspose(n=4, add_data_type=True).show()
#     movies.eTranspose(n=4, add_data_type=True, data_type='double').show()
#     movies.eTranspose(n=4, add_data_type=True, data_type='int').show()
#     movies.eTranspose(n=4, column_name_source="Title").show()
#     movies.select(*[col for i, col in enumerate(movies.columns) if i < 5]).eTranspose('Director', n=4).show()

#     with pytest.raises(ValueError):
#         movies.eTranspose(n=4, column_name_source="non existing column").show()


# def test_round(spark, movies):
#     movies.withColumn('IMDB Votes k', F.col('IMDB Votes') / 1000).eRound(0).show()

def test_remove_empty_columns(spark):
    df = spark.createDataFrame([
        (1, 'a', None),
        (2, None, None),
        (3, 'c', None)
    ], 'id INT, value STRING, empty_col STRING')

 
    assert len(df.eRemoveEmptyColumns().columns) == 2
    assert 'empty_col' not in df.eRemoveEmptyColumns().columns
>>>>>>> master
