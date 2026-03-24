import pathlib
import decimal
import html
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from datetime import datetime
from IPython.display import HTML as display_HTML
from IPython.display import display
from .DataFrameExtensions import DataFrameExtensions
from .DataFrameTap import DataFrameTap
from .Tools import Tools
from pyspark.sql import Row


class DataFrameDisplay():
    color_codes = dict(
        traffic_light_red = {'color': '#f7d3cc'},
        traffic_light_yellow = {'color': '#f7e998'},
        traffic_light_green = {'color': '#d3f7d3'},
        light0 = {'color': '#FFF0FF'},
        light1 = {'color': '#F0FFF0'},
        light2 = {'color': '#FFFFF0'},
        light3 = {'color': '#F0FFFF'},
        light4 = {'color': '#FFF0F0'},
        light5 = {'color': '#F0F0FF'},
        light6 = {'color': '#F0F0F0'},
    )

    style_codes = dict(
        bold = {'style': 'font-weight: bold;'},
        italic = {'style': 'font-style: italic;'},
        underline = {'style': 'text-decoration: underline;'},
    )

    # max_table_bytes = 500000
    max_table_bytes = 100000

    @staticmethod
    def set_colors(df, *color_rules: dict):
        assert all(isinstance(rule, dict) for rule in color_rules), "color_rules must be a list of dictionaries"
        allowed_keys = {'column', 'columns', 'color_code', 'style_code', 'condition'}
        cols = df.columns

        for i, rule in enumerate(color_rules):
            assert all(key in allowed_keys for key in rule.keys()), (
                "color_rules can only contain the following keys: {}".format(allowed_keys))
            assert not ('column' in rule and 'columns' in rule), (
                "color_rules cannot contain both 'column' and 'columns' keys")

            if 'column' in rule:
                rule['columns'] = [rule['column']]
                del rule['column']
            
            if 'columns' not in rule:
                rule['columns'] = cols

            assert all(col in cols for col in rule['columns']), (
                "column must be valid column in the DataFrame")

            assert 'color_code' in rule or 'style_code' in rule, (
                "color_rules must contain 'color_code' or 'style_code' or both")
            
            if 'color_code' in rule:
                assert isinstance(rule['color_code'], str) and rule['color_code'] in DataFrameDisplay.color_codes, (
                    "color_code must be one of the codes in DataFrameDisplay.color_codes")
            else:
                rule['color_code'] = None

            if 'style_code' in rule:
                assert isinstance(rule['style_code'], str) and rule['style_code'] in DataFrameDisplay.style_codes, (
                    "style_code must be one of the codes in DataFrameDisplay.style_codes")
            else:
                rule['style_code'] = None

            if 'condition' not in rule:
                rule['condition'] = F.lit(True)
            elif isinstance(rule['condition'], str):
                rule['condition'] = F.expr(rule['condition'])

            rule['i'] = i

        return (
            df
            # get the colors
            # step 1: create an array of colors and an array of styles
            # for each rule that applies to a column
            .withColumns({
                f"_{col}_color_code": 
                    F.array(*[
                        F.when(rule['condition'], F.lit(rule['color_code']))
                        for rule in color_rules if col in rule['columns']
                    ])
                for col in cols
            })
            .withColumns({
                f"_{col}_style_code": 
                    F.array(*[
                        F.when(rule['condition'], F.lit(rule['style_code']))
                        for rule in color_rules if col in rule['columns']
                    ])
                for col in cols
            })
            # step 2, compact the array's to remove null values and get the first color or 
            # style that applies to the column
            .withColumns({
                f"_{col}_color_code":
                    F.try_element_at(F.array_compact(F.col(f"_{col}_color_code")), F.lit(1))
                for col in cols
            })
            .withColumns({
                f"_{col}_style_code":
                    F.try_element_at(F.array_compact(F.col(f"_{col}_style_code")), F.lit(1))
                for col in cols
            })
            .withColumn(
                '_color_style', 
                F.array(*[
                    F.struct(
                        F.lit(col).alias('column'), 
                        F.col(f"_{col}_color_code").alias('color_code'), 
                        F.col(f"_{col}_style_code").alias('style_code'),
                    ) for col in cols
                ])
            )
            .drop(
                *[f"_{col}_color_code" for col in cols],
                *[f"_{col}_style_code" for col in cols]
            )
        )

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
            'lazy',
            'allow_additional_parameters',
            'color_rules',
        ]
        
        if 'allow_additional_parameters' in kwargs and kwargs['allow_additional_parameters']:
            valid_keys = list(set(valid_keys + list(kwargs.keys())))

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
                raise Exception(f"file_path must be a string, not {type(kwargs['file_path'])}")
            
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

        if 'lazy' in kwargs:
            if not isinstance(kwargs['lazy'], bool):
                raise Exception("lazy must be a boolean value")
        else:
            kwargs['lazy'] = True

        if 'color_rules' in kwargs:
            if not isinstance(kwargs['color_rules'], list):
                raise Exception("color_rules must be a list of dictionaries")
            
        return kwargs
        
    @staticmethod
    def data_to_html_table(df_collected, cols):
        # note we don't use tabulate here as we need to build the table body with additional functionality

        html_header = ''.join([f'<th>{html.escape(str(col))}</th>' for col in cols])
        html_body = ''
        for row in df_collected:
            html_body += '<tr>'
            for col in cols:
                value = DataFrameDisplay.cast_to_expandable_html(row[col])
                value = '' if not value else html.escape(str(value))
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
    def cast_to_expandable_html(data, add_quotes_when_needed=False, preview_prefix=None, preview_postfix=None):
        # print(type(data))

        if isinstance(data, Row):
            # Convert Row to a dictionary and handle it as a dict
            return DataFrameDisplay.cast_to_expandable_html(
                data.asDict(), 
                add_quotes_when_needed=True,
                # preview_prefix='Row(', 
                # preview_postfix=')'
            )

        # Handle Lists
        elif isinstance(data, list):
            # Create a single-line preview of the list
            preview = ", ".join(
                DataFrameDisplay.to_string(x, add_quotes_when_needed=True)
                for x in data
            )
            expanded_items = [
                DataFrameDisplay.cast_to_expandable_html(item, add_quotes_when_needed=True)
                for item in data
            ]
            multi_line = "".join(expanded_items)
            
            return DataFrameDisplay.expandable_html(preview, multi_line, preview_prefix='[', preview_postfix=']')


        # Handle Dictionaries (Optional, but useful for complex data)
        elif isinstance(data, dict):
            preview = ", ".join(f"{k}: {DataFrameDisplay.to_string(v, add_quotes_when_needed=True)}" for k, v in data.items())
            expanded_items = [f"<b>{k}:</b> {DataFrameDisplay.cast_to_expandable_html(v, add_quotes_when_needed=True)}" for k, v in data.items()]
            multi_line = "<br>".join(expanded_items)

            return DataFrameDisplay.expandable_html(
                preview, 
                multi_line, 
                preview_prefix=preview_prefix if preview_prefix else '{', 
                preview_postfix=preview_postfix if preview_postfix else '}'
            )
            
        # Handle basic data types (strings, ints, floats, etc.)
        else:
            return DataFrameDisplay.to_string(data, add_quotes_when_needed=add_quotes_when_needed)
        
    @staticmethod
    def to_string(value, add_quotes_when_needed=False):
        if add_quotes_when_needed and isinstance(value, str):
            return f"'{value}'"
        return str(value)

    @staticmethod
    def expandable_html(preview, multi_line, preview_prefix, preview_postfix, max_preview_length=60):
        if len(preview) > max_preview_length:
            preview = preview[:max_preview_length - 3] + "..."
        
        return f"""
        <details style="font-family: sans-serif;">
            <summary style="cursor: pointer;">{preview_prefix}{preview}{preview_postfix}</summary>
            <div style="padding-left: 25px; border-left: 2px solid #eee; margin-top: 3px; margin-bottom: 3px; line-height: 1.4;">
                {multi_line}
            </div>
        </details>
        """

    @staticmethod
    def collect_data_and_stats(df):
        cols = df.columns
        dtypes = df.dtypes
        df_collected = df.collect()

        stats = {col: {'type': dtype, 'length': 1, 'total': 0, 'decimals': 0, 'header_length': len(col)} for col, dtype in dtypes}
        for row in df_collected:
            for col in cols:
                value = row[col]
                if value is not None:
                    length = len(str(value))
                    stats[col]['length'] = max(stats[col]['length'], length)
                    stats[col]['total'] = stats[col]['total'] + length
                    if (isinstance(value, float) or isinstance(value, decimal.Decimal)) and str(value).split('.')[-1] != '0':
                        stats[col]['decimals'] = max(stats[col]['decimals'], len(str(value).split('.')[-1]))

        stats['__total__'] = {
            'rows': len(df_collected), 
            'columns': len(cols), 
            'width': sum([stats[col]['length'] for col in cols]),
            'avg_width': sum([stats[col]['total'] for col in cols]) / len(df_collected) if len(df_collected) > 0 else 0,
            'width_with_header': sum([max(stats[col]['length'], stats[col]['header_length']) for col in cols]),
            'size_limit': False,
        }

        # if the total (byte) size of the data is large we limited the number of rows to avoid browser performance issues
        if stats['__total__']['avg_width'] * stats['__total__']['rows'] > DataFrameDisplay.max_table_bytes and len(df_collected) > 1:
            new_n = int(DataFrameDisplay.max_table_bytes / stats['__total__']['avg_width'])
            df_collected = df_collected[:new_n]
            stats['__total__']['size_limit'] = True
            stats['__total__']['rows'] = new_n
        
        return df_collected, stats

    @staticmethod
    def display(df, *args, **kwargs):
        params = DataFrameDisplay.display_validate_parameters(df, *args, **kwargs)

        cols = df.columns
        has_rownum = '_rownum' in cols
        cols = [col for col in cols if col != '_rownum']
        dtypes = [dtype for dtype in df.dtypes if dtype[0] != '_rownum']
        nummeric_columns = [
            i for i, (col, dtype) in enumerate(dtypes)
            if Tools.check_data_type(dtype, 'num')
        ]

        # If sorting is requested, we do this the real way, with a rownum
        # if not requested but rownum is already present we use that
        # otherswise we just pick the first n rows and add a dummy rownum for the datatable
        if 'sort' in params:
            sort_by = DataFrameExtensions.transform_column_expressions(df, *params['sort'])
            df = df.withColumn('_rownum', F.row_number().over(W.orderBy(*sort_by)))
            df = df.filter(F.col('_rownum') <= params['n']).orderBy('_rownum')            
            ordering = f"order: [[{len(cols)}, 'asc']], ordering: true"
        elif has_rownum:
            # rownum to last position
            df = df.selectExpr('* except(_rownum)', '_rownum')
            df = df.filter(F.col('_rownum') < params['n'] + 1).orderBy('_rownum')            
            ordering = f"order: [[{len(cols)}, 'asc']], ordering: true"
        else:
            # pick random rows and add a dummy rownum for the datatable
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

        max_height = str(int(min(df_statistics['__total__']['rows'], params['page_length']) * 25 + 178)) + 'px'
        # Wrap in an iframe with srcdoc to enable proper JavaScript execution
        iframe_html = f"""
            <iframe srcdoc='{html_content.replace("'", "&apos;")}' 
                    width='99.9%' 
                    height='{max_height}px'
                    margin='0'
                    frameborder='0'
                    sandbox='allow-scripts allow-same-origin'
                    style='border: 1px solid #ddd; overflow-y: hidden; overflow-x: auto; display: block;'>
            </iframe>
        """            

        if params['display']:
            display(display_HTML(iframe_html))
        
        if params['passthrough']:
            return df
        elif DataFrameTap.tapped and DataFrameTap.tapped['end_on_display']:
            return DataFrameTap.tap_end()
