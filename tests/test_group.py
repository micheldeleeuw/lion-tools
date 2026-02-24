import os

def test_group(spark):
    path = os.path.dirname(__file__)
    path = os.path.join(path, "datasets", "movies.parquet")
    print('----')
    print('----')
    print('----')
    print('----')
    print('----')
    print(path)

    print('hello')
    # movies = spark.read.parquet(str(ipynbname.path().parent.joinpath("datasets", "movies.parquet")))
    # movies.show()