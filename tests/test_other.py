import pytest

def test_transpose(spark, movies):
    movies.eTranspose().show()