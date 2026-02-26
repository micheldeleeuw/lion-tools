import os
from pprint import pprint
import pytest
import pyspark.sql.functions as F

def test_group(spark):
    path = os.path.join(os.path.dirname(__file__), "..", "datasets", "movies.parquet")
    movies = spark.read.parquet(path)

    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix').agg().show(10)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').count().show(10)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('sum').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum().show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum('IMDB Rating', 'IMDB Votes').show(1)
    # with pytest.raises(AssertionError):
    #     movies.eGroup('Major Genre').totals('x').sum().show()
    
    (
        movies
        # .eP(lambda df: df.show(5))
        .filter('substr(`Major Genre`, 1, 1) in ("A")')
        .eGroup('Major Genre', 'Creative Type', add_rownum=True)
        .totals('Major Genre')
        # .totals('Major Genre', sections=True, grand_total=True)
        # .__dict__
        .agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)', 'sum(`Worldwide Gross`)')
        # .show(n=1000)
        .eC()
    )

    # movies.eGroup().agg(F.avg('IMDB Rating').alias('IMDB_Rating')).show(10)
    # (
    #     movies
    #     .select('Director', 'Distributor', 'IMDB Rating', 'IMDB Votes', 'MPAA Rating', 'Major Genre')
    #     .eGroup('Director', 'Distributor')
    #     .avg()
    #     # .filter('Director is not null and Distributor is not null')
    #     .filter('substr(Director, 1, 1) in ("A")')
    #     .eGroup('*', add_rownum=True)
    #     .totals('Director', grand_total=True)
    #     .agg(F.avg('IMDB Rating').alias('IMDB Rating'))
    #     .withColumn('rownum', F.col('_rownum'))
    #     # .show(10000)
    #     .eC()
    # )

    # from lion_tools import Cockpit
    # Cockpit.print_status()

