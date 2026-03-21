import pytest

from pyspark.sql import functions as F

def test_set_colors(spark, movies):

    movies = movies.select(*[col for col in movies.columns[0:5]])

    with pytest.raises(AssertionError):
        movies.eSetColors({'columns': ['Title']})

    movies.eSetColors(
        {'column': 'Director', 'color': 'red', 'style': 'bold'},
        {'columns': ['Director', 'Distributor'], 'color': '#123456'},
        {'color': 'red', 'condition': F.col('IMDB Rating') < 6, 'styles': ['bold']},
        {'columns': ['IMDB Votes'], 'color': '1', 'styles': ['italic']},
    ).show(truncate=False)

    # movies.eSetColors(
    #     {'color': 'red', 'condition': F.col('IMDB Votes') < 6},
    # ).show(truncate=False)
