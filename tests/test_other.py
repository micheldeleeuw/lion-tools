import pytest

def test_transpose(spark, movies):
    movies.eTranspose(n=4).show()
    movies.eTranspose(n=4, add_data_type=True).show()
    movies.eTranspose(n=4, add_data_type=True, data_type='double').show()
    movies.eTranspose(n=4, add_data_type=True, data_type='int').show()
    movies.eTranspose(n=4, column_name_source="Title").show()
    movies.select(*[col for i, col in enumerate(movies.columns) if i < 5]).eTranspose('Director', n=4).show()

    with pytest.raises(ValueError):
        movies.eTranspose(n=4, column_name_source="non existing column").show()
