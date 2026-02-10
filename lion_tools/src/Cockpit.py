from .settings import LION_TOOLS_PATH, LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files
from .settings import cleanup_temp_views
from .DataFrameDisplay import DataFrameDisplay
from datetime import datetime


class Cockpit():
    """
    The Lion Tools cockpit is a simple web application that displays the results 
    of Python/PySpark sessions running on the same machine, making the code you 
    run in those sessions more interactive and easier to work with. The cockpit is 
    designed to be simple and lightweight, and to work with minimal configuration. 

    Steps to use the cockpit:
    1. Start the cockpit by running `lion_tools cockpit` in your terminal. This
       will start the cockpit server and open the cockpit in that specific Python
       session, preferably in a notebook environment.
    2. In your Python/PySpark session, use the `display_in_cockpit` method to display 
       dataframes in the cockpit. For example:
            from lion_tools import DataFrameExtensions
            DataFrameExtensions.display_in_cockpit(my_dataframe)
    3. The cockpit will automatically pick up the display request and show the dataframe.
       You can interact with the dataframe, view its schema, and see the code that generated it.
    """

    @staticmethod
    def display_cockpit(_local_df, *args, **kwargs):
        # Dataframe is name _local_df to be able to find the name of the dataframe
        #  by going up the stack and looking for a variable with the same value.

        # clean old stuff up before doing anything, to avoid filling up
        # the temp directory and global temp view namespace
        cleanup_old_files()
        cleanup_temp_views()

        # validate parameters and set defaults
        if 'file_path' in kwargs:
            raise Exception("file_path parameter is not supported in display_in_cockpit, use display instead.")

        if 'display' not in kwargs:
            kwargs['display'] = False

        # as the cockpit will run in a different session, we need to validate and save the parameters
        # for the cockpit to pick up and do the actual display
        params = DataFrameDisplay.display_validate_parameters(_local_df, *args, **kwargs)

        if 'name' not in params:
            params['name'] = DataFrameExtensions.dataframe_name(_local_df)
        params['id'] = "_lion_tools_tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        params['temp_view_name'] = params['id'] + '_view'
        params['html_file'] = params['id'] + '.html'

        import pprint
        pprint.pprint(params)
        return

        # if not running in async mode, we create the html in the session and send it to the cockpit, 
        # otherwise we just send the parameters and let the cockpit do the work
        if 'async' in kwargs and not kwargs['async']:
            kwargs['file_path'] = params['html_file']
            DataFrameDisplay.display(_local_df, **params)
        else:
            # asynchronous display, evaluation happens in the cockpit, only queue
            # the display by saving the parameters
            cleanup_temp_views()

            # create temp view for the dataframe that can be used in de cockpit session
            params['physical_name'] = "_lion_tools_tmp_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            _local_df.createOrReplaceGlobalTempView(params['physical_name'])

        # write to LION_TOOLS_COCKPIT_PATH for the cockpit to pick up and display
        with open(LION_TOOLS_COCKPIT_PATH, 'a', encoding='utf-8') as f:
            f.write(str(params) + '\n')




    

