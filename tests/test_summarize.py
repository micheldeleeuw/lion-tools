import pytest

def test_summarize(spark, movies):
    df = spark.createDataFrame([(1, 2, 'xx'), (3, 4, 'yy')], ['a a', 'b()', 'c'])

    with pytest.raises(AssertionError):
        df.eSummarize(top=-1, stats=['min', 'max'])

    with pytest.raises(AssertionError):
        df.eSummarize(top=1, stats=['min', 'max', 'invalid_stat'])

    df.eSummarize(top=1).show()

    # movies.eSummarize(top=0).show()
