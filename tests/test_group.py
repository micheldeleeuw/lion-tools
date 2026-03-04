import pytest
from pprint import pprint
import pyspark.sql.functions as F

def test_group(spark, movies):
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix').agg().show(10)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').count().show(10)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('sum').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)').show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum().show(1)
    # movies.eGroup('-substr(`Director`, 1, 1) as DirectorPrefix', 'Major Genre').sum('IMDB Rating', 'IMDB Votes').show(1)
    # with pytest.raises(AssertionError):
    #     movies.eGroup('Major Genre').totals('x').sum().show()
    
    # (
    #     movies
    #     # .eP(lambda df: df.show(5))
    #     .filter('substr(`Major Genre`, 1, 1) in ("A")')
    #     .eGroup('Major Genre', 'Creative Type', add_rownum=True)
    #     .totals('Major Genre')
    #     # .totals('Major Genre', sections=True, grand_total=True)
    #     .agg('avg(`IMDB Rating`)', 'sum(`IMDB Votes`)', 'sum(`Worldwide Gross`)')
    #     .eC()
    # )

    # movies.eGroup().agg(F.avg('IMDB Rating').alias('IMDB_Rating')).show(10)
    aggregate = (
        movies
        .select('Director', 'Distributor', 'IMDB Rating', 'IMDB Votes', 'MPAA Rating', 'Major Genre')

        # .eGroup('Director', 'Distributor')
        # .avg()
        .filter('Director is not null and Distributor is not null')
        # .filter('substr(Director, 1, 1) in ("A")')

        .eGroup('*')
        .totals('Director', sections=True)
        .agg(F.avg('IMDB Rating').alias('IMDB Rating'))
        # .agg()
    )

    print(aggregate.columns)
    aggregate.show(20)

    # from lion_tools import Cockpit
    # Cockpit.print_status()

