import pytest
import time
import pyspark.sql.functions as F

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

def test_summary_2(spark):
    data = [
        [i, i * 2, f'bla_bla_{i % 5}', f'string_{i % 4}']
        for i in range(1001)
    ]
    data[2][2] = None

    df = (
        spark.createDataFrame(data, ['id', 'some_num', 'some_str', 'another_str'])
        .selectExpr(
            '*',
            'if(random() <0.05, null, cast(1000*random() as int)) as random_int',
            'round(10*random(), 2) as random_double',
        )
    )

    # df_summary = (
    #     df
    #     .eSummary(
    #         'some_str',
    #         stats=[
    #             "min", "max", "sum", "avg", "avg_null", "count_null",  "count_not_null",
    #             "count_distinct", "approx_count_distinct"
    #         ],
    #         top=2,
    #     )
    # )

    # assert df_summary.count() == 30
    # assert df_summary.columns == [
    #     'some_str', 'column_no', 'column', 'datatype', 'min', 'max', 
    #     'sum', 'avg', 'avg_null', 'count_null', 'count_not_null', 
    #     'count_distinct', 'approx_count_distinct', 'occurence_1', 'occurence_2'
    # ]

    base = (
        df
        .filter("not(id = 5 and some_num = 10)")
        .drop('random_int')
    )
    compare = (
        # df with some changes we will test to see if compare_summary will detect them
        df
        .withColumn('some_num', F.when(F.col('id') == 5, 999).otherwise(F.col('some_num')))
    )
    
    (
        base
        .eCompareSummary(
            compare, 
            # 'some_str',
            stats=[
                "min", "max", "sum", "avg", "avg_null", "count_null",  "count_not_null",
                "count_distinct", "approx_count_distinct"
            ],
            ignore_missing_columns=False,
            round_decimals=2,
        )
        .eC()
    )

