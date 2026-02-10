import pathlib
import decimal
import html
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from datetime import datetime
from IPython.display import HTML as display_HTML
from IPython.display import display



class DataFrameDisplay():

    @staticmethod
    def display_validate_parameters(df, *args, **kwargs):
        if not ('pyspark.sql' in str(type(df)) and 'DataFrame' in str(type(df))):
            raise Exception("This method can only be used on a pyspark DataFrame")

        valid_keys = [
            'name',
            'n',
            'passthrough',
            'file_path',
            'sort',
            'p',
            'page_length',
            'display',
            'async',
        ]
        
        for key in kwargs:
            # note p is an alias for page_length
            assert key in valid_keys, "Unknown parameter: {}".format(key)
                
        # this is nasty but allows for positional arguments which is really helpful for the user
        for val in args:
            if isinstance(val, bool) and 'passthrough' not in kwargs:
                kwargs['passthrough'] = val
            elif isinstance(val, int) and 'n' not in kwargs:
                kwargs['n'] = val
            elif isinstance(val, str) and 'name' not in kwargs:
                kwargs['name'] = val
            else:
                raise Exception("Unknown positional argument: {}".format(val))

        if 'display' not in kwargs:
            kwargs['display'] = True
        else:
            assert isinstance(kwargs['display'], bool), "display must be a boolean value"

        if 'name' in kwargs:
            if not isinstance(kwargs['name'], str):
                raise Exception("name must be a string")

        if 'n' in kwargs:
            if not isinstance(kwargs['n'], int) or kwargs['n'] < 1 or kwargs['n'] > 100000:
                raise Exception("n must be an integer between 1 and 100.000")
        else:
            kwargs['n'] = 1001 # max 1.001 rows is standard

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

        if 'p' in kwargs and 'page_length' not in kwargs:
            kwargs['page_length'] = kwargs['p']
            del kwargs['p']

        if 'page_length' in kwargs:
            assert (
                isinstance(kwargs['page_length'], int) and kwargs['page_length'] > 0 and kwargs['page_length'] <= 1000000
            ), "page_length must be a positive integer between 1 and 100.000"
        else:
            kwargs['page_length'] = 15

        if 'display' in kwargs:
            if not isinstance(kwargs['display'], bool):
                raise Exception("display must be a boolean value")
        else:
            kwargs['display'] = True

        if 'async' in kwargs:
            if not isinstance(kwargs['async'], bool):
                raise Exception("async must be a boolean value")
        else:
            kwargs['async'] = True

        return kwargs
        
    @staticmethod
    def data_to_html_table(df_collected, cols):
        # note we don't use tabulate here as we need to build the table body with additional functionality

        html_header = ''.join([f'<th>{html.escape(str(col))}</th>' for col in cols])
        html_body = ''
        for row in df_collected:
            html_body += '<tr>'
            for col in cols:
                value = row[col]
                value = '' if not value else html.escape(str(value))
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

        stats = {col: {'type': dtype, 'length': 1, 'decimals': 0, 'header_length': len(col)} for col, dtype in dtypes}
        for row in df_collected:
            for col in cols:
                value = row[col]
                if value is not None:
                    # print(col, type(value), str(value))
                    stats[col]['length'] = max(stats[col]['length'], len(str(value)))
                    if (isinstance(value, float) or isinstance(value, decimal.Decimal)) and str(value).split('.')[-1] != '0':
                        stats[col]['decimals'] = max(stats[col]['decimals'], len(str(value).split('.')[-1]))

        stats['__total__'] = {
            'rows': len(df_collected), 
            'columns': len(cols), 
            'width': sum([stats[col]['length'] for col in cols]),
            'width_with_header': sum([max(stats[col]['length'], stats[col]['header_length']) for col in cols]),
        }

        return df_collected, stats

    @staticmethod
    def display(df, *args, **kwargs):
        params = DataFrameDisplay.display_validate_parameters(df, *args, **kwargs)

        cols = df.columns
        dtypes = df.dtypes
        nummeric_columns = [
            i for i, (col, dtype) in enumerate(dtypes)
            if dtype in ['int', 'bigint', 'double', 'float', 'decimal'] or dtype.startswith('decimal(')
        ]

        # If sorting is requested, we do this the real way, with a rownum
        if 'sort' in params:
            sort_by = DataFrameDisplay.sort_transform_expressions(df, *params['sort'])
            df = df.withColumn('_rownum', F.row_number().over(W.orderBy(*sort_by)))
            df = df.filter(F.col('_rownum') <= params['n']).orderBy('_rownum')            
            ordering = f"order: [[{len(cols)}, 'asc']], ordering: true"
        else:
            df = df.limit(params['n']).withColumn('_rownum', F.lit(0))
            ordering = "ordering: true"
            
        df_collected, df_statistics = DataFrameDisplay.collect_data_and_stats(df)
        columns_popup = str(list([
            html.escape(
                col + '---(' + dtype + ')'
                if len(dtype) <= 25
                else col + '---(' + dtype[0:22] + '...)'
            )
            for col, dtype in dtypes
        ]))

        cols_defs_rownum = f"{{ targets: [{len(cols)}], visible: false,  searchable: false}}"
        col_defs_alignment_right = f'''{{ targets: {str(nummeric_columns + [len(cols)])}, className: 'dt-right' }}'''
        col_defs_number_format = ''
        for i, col in enumerate(nummeric_columns):
            col_name = cols[col]
            decimals = df_statistics[col_name]['decimals']
            if i>0:
                col_defs_number_format += ',\n            '
            col_defs_number_format += f"{{ targets: [{col}], render: $.fn.dataTable.render.number( ',', '.', {decimals}, '', '&nbsp;&nbsp;' ) }}"
            # (number_format, thousands_sep, decimals, prefix, suffix)
        col_defs_number_format = '{}' if col_defs_number_format == '' else col_defs_number_format
        
        if df_statistics['__total__']['width_with_header'] * 8 + 50 < 600:
            max_width = '600px'
        else:
            max_width = str(df_statistics['__total__']['width_with_header'] * 9 + 50) + 'px'  # rough estimate of width in pixels

        _options = sorted([5, 50, params['page_length']])
        _options = list(dict.fromkeys(_options))
        length_menu = str([[*_options, -1], [*_options, "All"]])

        # Load template using relative path from this file's location
        with open(pathlib.Path(__file__).parent.parent / "templates" / "dataframe_view_template.html", 'r', encoding='utf-8') as f:
            html_content = f.read()

        # create html
        html_content = html_content.replace('{generation_date}', datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        html_content = html_content.replace('{main_table}', DataFrameDisplay.data_to_html_table(df_collected, df.columns))
        html_content = html_content.replace('{columns}', columns_popup)
        html_content = html_content.replace('{col_defs_alignment_right}', col_defs_alignment_right)        
        html_content = html_content.replace('{col_defs_number_format}', col_defs_number_format)
        html_content = html_content.replace('{col_defs_rownum}', cols_defs_rownum)
        html_content = html_content.replace('{ordering}', ordering)
        html_content = html_content.replace('{max_width}', max_width)
        html_content = html_content.replace('{page_length}', str(params['page_length']))
        html_content = html_content.replace('{length_menu}', length_menu)

        if 'file_path' in params:
            # save to file
            with open(params['file_path'], 'w', encoding='utf-8') as f:
                f.write(html_content)

        max_height = str(int(min(df_statistics['__total__']['rows'], params['page_length']) * 25 + 165)) + 'px'
        # Wrap in an iframe with srcdoc to enable proper JavaScript execution
        iframe_html = f"""
            <iframe srcdoc='{html_content.replace("'", "&apos;")}' 
                    width='100%' 
                    height='{max_height}px'
                    margin='0'
                    frameborder='0'
                    sandbox='allow-scripts allow-same-origin'
                    style='border: 1px solid #ddd;'>
            </iframe>
        """            

        print(kwargs)
        if params['display']:
            display(display_HTML(iframe_html))
        
        elif params['passthrough']:
            return df
        else:
            return
