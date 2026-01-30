import pathlib
from IPython.display import HTML as display_HTML
from datetime import datetime
import decimal
import pyspark.sql.functions as F


class DataFrameExtensions():

    @staticmethod
    def extend_dataframe():

        global DataFrame
        from pyspark.sql import DataFrame

        DataFrame.d = DataFrameExtensions.d
        DataFrame.display = DataFrameExtensions.display
        DataFrame.sort = DataFrameExtensions.sort


    def __init__(self):
        print('Use extend_dataframe() to extend DataFrame functionality.')

    
    @staticmethod
    def sort(df, *args):
        cols = df.columns
        _args = [a if isinstance(a, str) else ("-" if a<0 else "") + cols[abs(a) - 1] for a in args]

        order_by = []
        for col_expr in _args:
            if col_expr[0:1] == '-':
                order_by.append(F.expr(f'{col_expr[1:]}').desc() )
            else:
                order_by.append(F.expr(f'{col_expr}') )
        return df.orderBy(*order_by)


    @staticmethod
    def d(df, *args, **kwargs):
        DataFrameExtensions.display(df, *args, **kwargs)


    @staticmethod
    def display_validate_parameters(df, *args, **kwargs):
        if not ('pyspark.sql' in str(type(df)) and 'DataFrame' in str(type(df))):
            raise Exception("This method can only be used on a pyspark DataFrame")
        
        for key in kwargs:
            assert key in ['n', 'passthrough', 'file_path'], "Unknown parameter: {}".format(key)
                
        # this is nasty but allows for positional arguments which is rely helpful for the user
        for val in args:
            if isinstance(val, int) and 'n' not in kwargs:
                kwargs['n'] = val
            elif isinstance(val, bool) and 'passthrough' not in kwargs:
                kwargs['passthrough'] = val
            else:
                raise Exception("Unknown positional argument: {}".format(val))

        if 'n' in kwargs:
            if not isinstance(kwargs['n'], int) or kwargs['n'] < 1 or kwargs['n'] > 100000:
                raise Exception("n must be an integer between 1 and 100.000")
        else:
            kwargs['n'] = 10000 # max 10k rows is standard

        if 'passthrough' in kwargs:
            if not isinstance(kwargs['passthrough'], bool):
                raise Exception("passthrough must be a boolean value")
        else:
            kwargs['passthrough'] = False

        if 'file_path' in kwargs:
            if not isinstance(kwargs['file_path'], str):
                raise Exception("file_path must be a string")
            
        if 'sort' in kwargs:
            if not isinstance(kwargs['sort'], list):
                kwargs['sort'] = [kwargs['sort']]

            assert all([
                isinstance(i, (str, int))
                for i in kwargs['sort']
            ]), "sort values must be strings or integers"
            

        return kwargs
        
    @staticmethod
    def data_to_html_table(df_collected):
        # note we don't use tabulate here as we need to build the table body with additional functionality
        cols = df_collected[0].asDict().keys()
        
        html_header = ''.join([f'<th>{col}</th>' for col in cols])
        html_body = ''
        for row in df_collected:
            html_body += '<tr>'
            for col in cols:
                value = row[col]
                value = '' if not value else value
                # value = 'null&nbsp;&nbsp;' if not value else value
                html_body += f'<td>{value}</td>'
            html_body += '</tr>\n'
                

        html_table = f"""
            <table id="mainTable" class="display" style="width:100%">
                <thead><tr>{html_header}</tr></thead>
                <tbody>
                    {html_body}
                </tbody>
            </table>
        """

        return html_table
    
    @staticmethod
    def collect_data_and_stats(df):
        cols = df.columns
        dtypes = df.dtypes
        df_collected = df.collect()

        stats = {col: {'type': dtype, 'length': 1, 'decimals': 0} for col, dtype in dtypes}
        for row in df_collected:
            for col in cols:
                value = row[col]
                if value is not None:
                    # print(col, type(value), str(value))
                    stats[col]['length'] = max(stats[col]['length'], len(str(value)))
                    if (isinstance(value, float) or isinstance(value, decimal.Decimal)) and str(value).split('.')[-1] != '0':
                        stats[col]['decimals'] = max(stats[col]['decimals'], len(str(value).split('.')[-1]))

        stats['__total__'] = {'rows': len(df_collected), 'columns': len(cols), 'width': sum([stats[col]['length'] for col in cols])}

        return df_collected, stats


    @staticmethod
    def display(df, *args, **kwargs):
        params = DataFrameExtensions.display_validate_parameters(df, *args, **kwargs)

        cols = df.columns
        dtypes = df.dtypes
        nummeric_columns = [
            i for i, (col, dtype) in enumerate(dtypes)
            if dtype in ['int', 'bigint', 'double', 'float', 'decimal'] or dtype.startswith('decimal(')
        ]
        if 'sort' in params:
            df = DataFrameExtensions.sort(df, *params['sort'])

        df_collected, df_statistics = DataFrameExtensions.collect_data_and_stats(df.limit(params['n']))
        col_defs_alignment_right = f'''{{ targets: {str(nummeric_columns)}, className: 'dt-right' }}'''
        col_defs_number_format = ''
        for col in nummeric_columns:
            col_name = cols[col]
            decimals = df_statistics[col_name]['decimals']
            col_defs_number_format += f'''{{ targets: [{col}], render: $.fn.dataTable.render.number( ',', '.', {decimals}, '', '&nbsp;&nbsp;' ) }},\n            '''
            # (number_format, thousands_sep, decimals, prefix, suffix)

        # Load template using relative path from this file's location
        with open(pathlib.Path(__file__).parent.parent / "templates" / "dataframe_view_template.html", 'r', encoding='utf-8') as f:
            html_content = f.read()

        # create html
        html_content = html_content.replace('{generation_date}', datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        html_content = html_content.replace('{main_table}', DataFrameExtensions.data_to_html_table(df_collected))
        html_content = html_content.replace('{columns}', str(list([col + '---(' + dtype + ')' for col, dtype in dtypes])))
        html_content = html_content.replace('{col_defs_alignment_right}', col_defs_alignment_right)        
        html_content = html_content.replace('{col_defs_number_format}', col_defs_number_format)

        if 'file_path' in params:
            # save to file
            with open(params['file_path'], 'w', encoding='utf-8') as f:
                f.write(html_content)

        # Wrap in an iframe with srcdoc to enable proper JavaScript execution
        iframe_html = f"""
            <iframe srcdoc='{html_content.replace("'", "&apos;")}' 
                    width='100%' 
                    height='600' 
                    frameborder='0'
                    style='border: 1px solid #ddd;'>
            </iframe>
        """
        
        result = display_HTML(iframe_html)
        
        if params['passthrough']:
            return df
        else:
            return result

