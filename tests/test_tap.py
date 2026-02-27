import pytest
import os
from lion_tools import DataFrameTap

def test_tap(spark, movies, alice_bob):    
    assert DataFrameTap.tapped is None
    movies.eTap()
    assert DataFrameTap.tapped is not None
    DataFrameTap.show_tap()
    assert DataFrameTap.tapped['df'].count() == movies.count()
    assert alice_bob.eTapEnd().count() == movies.count()

def test_tap_no_active_tap(spark):
    with pytest.raises(ValueError):
        DataFrameTap.tap_end()

def test_tap_end_on_display(spark,movies):
    assert movies.eTap().limit(3).eD().count() == movies.count()
    assert isinstance(movies.eTap(end_on_display=False).limit(3).eD(), type(None))
    DataFrameTap.tap_end()
    
    with pytest.raises(ValueError):
        DataFrameTap.tap_end()
    
def test_tab_inline(spark, alice_bob):
    def check_columns(df):
        assert len(df.columns) == 2

    # two checks in one line, just for fun
    assert alice_bob.eTap(check_columns).count() == 2