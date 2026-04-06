import pytest
import time

# def test_summary(spark, movies):
#     df = spark.createDataFrame([(1, 2, 'xx'), (3, 4, 'yy'), (None, None, 'xx')], ['a a', 'b()', 'c'])

#     with pytest.raises(AssertionError):
#         df.eSummary(top=-1, stats=['min', 'max'])

#     with pytest.raises(AssertionError):
#         df.eSummary(top=1, stats=['min', 'max', 'invalid_stat'])

#     df.eSummary(top=1).show()
#     movies.eSummary('Director').show()

# def test_top(spark, movies):
#     movies.eTop().show()
#     movies.eTop(n=10, transpose=True).show()
#     movies.eTop('Director').show()

def test_summary_of_summary(spark):
    data = [
        [i, i * 2, f'bla_bla_{i % 5}', f'string_{i % 4}']
        for i in range(1001)
    ]
    data[2][2] = None
    df = spark.createDataFrame(data, ['id', 'some_num', 'some_str', 'another_str'])

    (
        df
        .selectExpr(
            '*',
            'if(random() <0.05, null, cast(1000*random() as int)) as random_int',
            'round(10*random(), 2) as random_double',
        )
        .eSummary(
            'some_str',
            stats=[
                "min", "max", "sum", "avg", "avg_null", "count_null",  "count_not_null",
                "count_distinct", "approx_count_distinct"
            ],
            top=2,
        )
        .eC(name=time.strftime("%H:%M:%S"))
    )

