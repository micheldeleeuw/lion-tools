from pyspark.sql import DataFrame
from typing import Callable
import datetime

class DataFrameTap():
    tapped = None

    @classmethod
    def tap(cls, df: DataFrame, f: Callable[[DataFrame], None] = None, end_on_display: bool = True) -> DataFrame:
        """
        Taps can be inline or block (out-of-line).

        Examples:
        - inline: df.eTap(lambda df: print(df.count())).show()
        - block: df.eTap().eGroup('col1').count().eC().eC() # tap ends on display, so no need to call eTapEnd()
        - block: df.eTap(end_on_display=False).eGroup('col1').count().eC().eTapEnd().eC()

        """
        if f is not None:
            # inline tap, execute the function immediately and return the original dataframe
            f(df)
        else:
            # block tap, store the dataframe and timestamp for later use
            cls.tapped = dict(
                df = df,
                timestamp = datetime.datetime.now(),
                end_on_display = end_on_display,
            )
        
        return df

    @classmethod
    def tap_end(cls) -> DataFrame:
        if cls.tapped is None:
            raise ValueError("No tap active.")
    
        df = cls.tapped['df']
        cls.tapped = None

        return df
    
    @classmethod
    def show_tap(cls) -> None:
        if cls.tapped is None:
            print("No tap active.")
            return
        
        print(f"Tap started at: {cls.tapped['timestamp']}: ")
        print(cls.tapped['df'].columns)