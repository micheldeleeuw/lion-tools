import os
import pytest
import pyspark.sql.functions as F

def test_group(spark):
    path = os.path.join(os.path.dirname(__file__), "..", "datasets", "movies.parquet")
    movies = spark.read.parquet(path)

    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg().show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('sum').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum().show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum('IMDB Rating', 'IMDB Votes').show(1)
    # with pytest.raises(AssertionError):
    #     movies.eGroup('Major Genre').totals('x').sum().show()
    
    (
        movies
        .eGroup('Major Genre', 'Creative Type', add_rownum=True)
        .totals('Major Genre', grand_total=True)
        .agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)', 'sum(`Worldwide Gross`)')
        .show(n=1000)
    )

    # movies.groupBy('*').agg(F.sum('IMDB Rating').alias('sum_IMDB_Rating')).show(10)


