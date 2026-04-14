import pathlib
import decimal
import html
from pprint import pprint
from sys import prefix
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from datetime import datetime
from IPython.display import HTML as display_HTML
from IPython.display import display
from .DataFrameExtensions import DataFrameExtensions
from .DataFrameTap import DataFrameTap
from .Tools import Tools
from pyspark.sql import Row
from itertools import groupby
import re

class DataFrameDisplay():
    color_codes_colors = [
        # Grays
        "slate", "gray", "zinc", "neutral", "stone",
        # Colors
        "red", "orange", "amber", "yellow", "lime", "green", "emerald", 
        "teal", "cyan", "sky", "blue", "indigo", "violet", "purple", 
        "fuchsia", "pink", "rose"
    ]

    color_codes_weights = [
        "50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"
    ]
    
    color_codes_weight_mapping_background_to_font = {
        '50': '900', '100': '900', '200': '950', '300': '950', '400': '950', '500': '50',
        '600': '50', '700': '50', '800': '50', '900': '50', '950': '100'
    }

    style_codes = [
        'underline', 'overline', 'line-through', 'no-underline',
        'decoration-solid', 'decoration-double', 'decoration-dotted', 'decoration-dashed', 'decoration-wavy',
        'uppercase', 'lowercase', 'capitalize', 'normal-case',
        'font-sans', 'font-serif', 'font-mono',
        'font-thin', 'font-bold', 'font-black',
        'italic', 'not-italic',
        'antialiased', 'subpixel-antialiased',
    ]

    # max_table_bytes = 500000
    max_table_bytes = 200000

    @staticmethod
    def set_colors(df, *color_rules: dict):
        if len(color_rules) == 0:
            raise ValueError("At least one color rule must be provided")
        
        if isinstance(color_rules[0], list):
            raise ValueError("Color rules should be provided as separate arguments, not as a list.")
        
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
                if len(rule['color_code'].split('-')) == 1:
                    # both background and font weight are not set, set background to 100
                    rule['color_code'] += '-100'
                if len(rule['color_code'].split('-')) == 2:
                    # font weight is not set, set it to a value that works with the background weight
                    rule['color_code'] += ('-' + 
                        DataFrameDisplay.color_codes_weight_mapping_background_to_font.get(rule['color_code'].split('-')[1], 'x'))

                color_code = rule['color_code']
                error_message = (
                    "color_code must be in the tailwind CSS format `color(-xxx-yyy)` where color is one of"
                    f"\n{DataFrameDisplay.color_codes_colors}"
                    f"\nand xxx and yyy (both optional) are one of {DataFrameDisplay.color_codes_weights}"
                    "\nwhere xxx is the background weight and yyy is the font weight. See https://tailwindcss.com/docs/colors for details."
                    f"\nEvaluated color_code: {color_code}"
                )

                assert isinstance(color_code, str) and len(color_code.split('-')) in (2, 3), error_message
                assert color_code.split('-')[0] in DataFrameDisplay.color_codes_colors, error_message
                assert color_code.split('-')[1] in DataFrameDisplay.color_codes_weights, error_message
                assert color_code.split('-')[2] in DataFrameDisplay.color_codes_weights, error_message

            else:
                rule['color_code'] = None

            if 'style_code' in rule:
                assert isinstance(rule['style_code'], str) and all(code in DataFrameDisplay.style_codes for code in rule['style_code'].split(" ")), (
                    f"style_code must be one of {DataFrameDisplay.style_codes}")
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
            'add_time_to_name',
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
            'pretty_headers',
            'format_totals',
            'column_grouping',
            'column_grouping_split_pattern',
            'percentage_columns_patterns',
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

        if 'add_time_to_name' in kwargs:
            if not isinstance(kwargs['add_time_to_name'], bool):
                raise Exception("add_time_to_name must be a boolean value")
        else:
            kwargs['add_time_to_name'] = False
                    
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

        if 'format_totals' in kwargs:
            if not isinstance(kwargs['format_totals'], bool):
                raise Exception("format_totals must be a boolean value")
        else:
            kwargs['format_totals'] = True

        if 'color_rules' in kwargs:
            if isinstance(kwargs['color_rules'], dict):
                kwargs['color_rules'] = [kwargs['color_rules']]
                
            if not isinstance(kwargs['color_rules'], list):
                raise Exception("color_rules must be a list of dictionaries")
            
        if 'pretty_headers' in kwargs:
            if not isinstance(kwargs['pretty_headers'], bool):
                raise Exception("pretty_headers must be a boolean value")
            
        if 'column_grouping' in kwargs:
            if not isinstance(kwargs['column_grouping'], bool):
                raise Exception("column_grouping must be a boolean value")
            
        if 'column_grouping_split_pattern' in kwargs:
            if not isinstance(kwargs['column_grouping_split_pattern'], str):
                raise Exception("column_grouping_split_pattern must be a string value")
            
        if 'percentage_columns_pattern' in kwargs:
            assert isinstance(kwargs['percentage_columns_pattern'], str), "percentage_columns_pattern must be a string value representing a regex pattern to identify percentage columns"
        
        return kwargs
        
    @staticmethod
    def data_to_html_table(df_collected, headers, cols):
        # note we don't use tabulate here as we need to build the table body with additional functionality
        cols = [col for col in cols if col not in ('_totals_type', '_color_style')]

        headers_ext = [
            [
                i,
                'single' if i==0 and len(headers) == 1
                else 'last' if i == len(headers) - 1
                else 'non_last', 
                header_row
            ]
            for i, header_row in enumerate(headers)
        ]

        # table header
        table_header = "\n                    ".join(
            '<tr>' + 
                ''.join([
                    f'<th>{html.escape(str(header))}</th>' if header_type in ('last', 'single')
                    else f'<th>{html.escape(str(header))}</th>' if colspan == 1 and i % 2 == 0
                    else f'<th class="grouped_column">{html.escape(str(header))}</th>' if colspan == 1 and i % 2 == 1
                    else f'<th colspan="{colspan}">{html.escape(str(header))}</th>' if i % 2 == 0
                    else f'<th colspan="{colspan}" class="grouped_column">{html.escape(str(header))}</th>'
                    for i, (colspan, header) in enumerate(header_row)
                ]) +
            '</tr>'
            for i, header_type, header_row in headers_ext    
        )

        # table body
        table_body = ''
        for row in df_collected:
            table_body += '<tr>'
            for col in cols:
                # style
                style_str = ''
                if '_color_style' in row and row['_color_style'] is not None:
                    style_info = next((row.asDict() for row in row['_color_style'] if row['column'] == col), None)
                    if style_info is not None:
                        color_code = style_info.get('color_code', None)
                        style_code = style_info.get('style_code', None)
                        if color_code is not None:
                            color_parts = color_code.split('-')
                            background_color = f"bg-{color_parts[0]}-{color_parts[1]}"
                            text_color = f"text-{color_parts[0]}-{color_parts[2]}"
                            style_str += f"{background_color} {text_color} "
                        if style_code is not None:
                            style_str += f"{style_code} "

                # value
                value = DataFrameDisplay.cast_to_expandable_html(row[col])
                if style_str != '':
                    table_body += f'<td class="{style_str.strip()}">{value}</td>'
                else:
                    table_body += f'<td>{value}</td>'
            table_body += '</tr>\n'
                

        # bring it together
        html_table = f"""
            <table id="mainTable" class="display" style="width:100%">
                <thead>
                    {table_header}
                </thead>
                <tbody>
                    {table_body}
                </tbody>
            </table>
        """

        return html_table
    
    @staticmethod
    def cast_to_expandable_html(data, add_quotes_when_needed=False, preview_prefix=None, preview_postfix=None):
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

            if (
                len(data) > 0 and 
                (isinstance(data[0], Row) or isinstance(data[0], dict) or isinstance(data[0], list))
            ):
                expanded_items = [
                    DataFrameDisplay.cast_to_expandable_html(item, add_quotes_when_needed=True)
                    for item in data
                ]
                multi_line = "".join(expanded_items)
            else:
                expanded_items = [
                    "- " + DataFrameDisplay.cast_to_expandable_html(item, add_quotes_when_needed=True)
                    for item in data
                ]
                multi_line = "<br>".join(expanded_items)
            
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
            return f"'{html.escape(value)}'"
        elif add_quotes_when_needed and value is None:
            return 'null'
        elif value is None:
            return ''
        elif isinstance(value, str) and len(value) == 32 and all(c in '0123456789abcdefABCDEF' for c in value):
            value = html.escape(value)
            return value[0:5] + "...." + value[-5:]
        else:
            return html.escape(str(value))

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
    def remove_unnecessary_sub_totals(df_collected):
        # Unnecessary sub-totals are sub totals where there only is one record feeding the sub total.
        group_record_count = 0
        df_collected_new = []
        df_collected_len =len(df_collected)

        for rownum, row in enumerate(df_collected):
            if row['_totals_type'] <= 2: 
                # regular rows
                group_record_count += 1

            if row['_totals_type'] == 3 and group_record_count <= 1:
                # unnecessary sub total row when the group has only one record
                pass
            elif (
                row['_totals_type'] == 4 and 
                group_record_count <= 1 and 
                (
                    ((rownum + 2) < df_collected_len and df_collected[rownum + 2]['_totals_type'] > 2) or
                    (rownum + 2) >= df_collected_len
                )
            ):
                # unnecessary empty row when both the previous and next group have only one record
                # or when this is the last row in the table and the previous group has only one record
                pass
            elif row['_totals_type'] == 5 and rownum == df_collected_len - 1:
                # empty row at end of table coming from sections
                pass
            else:
                # normal row or necessary sub total/empty row
                df_collected_new.append(row)

            if row['_totals_type'] == 4: 
                group_record_count = 0

        return df_collected_new                    

    @staticmethod
    def collect_data_and_stats(df, all_cols, cols, dtypes):
        cols = df.columns
        stat_cols = [col for col in cols if col not in ('_totals_type', '_color_style')]
        dtypes = df.dtypes
        df_collected = df.collect()
        stats = {col: {'type': dtype, 'length': 1, 'total': 0, 'decimals': 0, 'header_length': len(col)} for col, dtype in dtypes if col in stat_cols}

        if '_totals_type' in all_cols:
            df_collected = DataFrameDisplay.remove_unnecessary_sub_totals(df_collected)

        for row in df_collected:
            for col in stat_cols:
                value = row[col]
                if value is not None:
                    length = len(str(value))
                    stats[col]['length'] = max(stats[col]['length'], length)
                    stats[col]['total'] = stats[col]['total'] + length
                    if (isinstance(value, float) or isinstance(value, decimal.Decimal)) and str(value).split('.')[-1] != '0':
                        stats[col]['decimals'] = max(stats[col]['decimals'], len(str(value).split('.')[-1]))

        stats['__total__'] = {
            'rows': len(df_collected), 
            'columns': len(stat_cols), 
            'width': sum([stats[col]['length'] for col in stat_cols]),
            'avg_width': sum([stats[col]['total'] for col in stat_cols]) / len(df_collected) if len(df_collected) > 0 else 0,
            'width_with_header': sum([max(stats[col]['length'], stats[col]['header_length']) for col in stat_cols]),
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
    def get_headers(cols, pretty_headers, column_grouping, column_grouping_split_pattern):
        # If column grouping is enabled we add an additional row in the header containing the group name.
        # The group name is derived from the column name by taking the part before the first occurrence of a split pattern. 
        # Note that the page length needs to be corrected to account for multi row headers.
        # Pretty headers are basically just a cosmetic change to make the headers more readable by replacing underscores with
        # spaces and capitalizing words.

        header_length = max([len(col.split(column_grouping_split_pattern)) for col in cols])

        if column_grouping and header_length > 1:
            split_cols = [col.split(column_grouping_split_pattern) for col in cols]
            split_cols = [
                split_col if not pretty_headers else [split_col_.replace('_', ' ').title() for split_col_ in split_col]
                for split_col in split_cols
            ]
            # pad the arrays with empty string
            split_cols = [[' '] * (header_length - len(split_col)) + split_col for split_col in split_cols]
            # transpose the matrix
            header_rows = list(map(list, zip(*split_cols)))
            # make single titles with count of the number columns the title must span
            headers = [
                [
                    [len(list(group)), label]
                    for label, group in groupby(header_row)
                ] 
                for header_row in header_rows
            ]
            uneven_columns = []
            i = 0
            for j, col in enumerate(headers[-2]):
                if j % 2 == 1:
                    for k in range(col[0]):
                        uneven_columns.append(i + k)
                i += col[0]

        else:
            # make just the single row
            header_length = 1
            uneven_columns = []
            headers = [[
                [1, col] if not pretty_headers else [1, col.replace('_', ' ').title()]
                for col in cols
            ]]

        return headers, header_length, uneven_columns
    
    @staticmethod
    def columns_definitions(cols, nummeric_columns, percentage_columns_pattern, df_statistics, uneven_columns):
        cols_defs = {}
        cols_defs['rownum'] = [len(cols)]
        cols_defs['alignment_right'] = nummeric_columns + [len(cols)]
        cols_defs['grouped_columns'] = uneven_columns

        for i, col in enumerate(cols):
            if i in nummeric_columns and re.search(percentage_columns_pattern, col):
                # nummeric + %
                decimals = df_statistics[col]['decimals']
                cols_defs.setdefault('number_format%', {})
                cols_defs['number_format%'].setdefault(decimals, [])
                cols_defs['number_format%'][decimals].append(i)
            elif i in nummeric_columns:
                # nummeric
                decimals = df_statistics[col]['decimals']
                cols_defs.setdefault('number_format', {})
                cols_defs['number_format'].setdefault(decimals, [])
                cols_defs['number_format'][decimals].append(i)
            elif re.search(percentage_columns_pattern, col):
                # not nummeric + %
                cols_defs.setdefault('string_format%', [])
                cols_defs['string_format%'].append(i)
            else:
                # not nummeric
                cols_defs.setdefault('string_format', [])
                cols_defs['string_format'].append(i)

        col_defs_str = []
        for key, value in cols_defs.items():
            if key == 'rownum':
                col_defs_str.append(f"{{ targets: {value}, visible: false,  searchable: false }},")
            elif key == 'alignment_right':
                col_defs_str.append(f"{{ targets: {value}, className: 'dt-right' }},")
            elif key == 'grouped_columns':
                col_defs_str.append(f"{{ targets: {value}, className: 'grouped_column' }},")
            elif key == 'number_format%':
                for decimals, cols in value.items():
                    col_defs_str.append(
                        f"{{ targets: {cols}, render: $.fn.dataTable.render.number( ',', '.', {decimals}, '&nbsp;', '%&nbsp;' ) }},"
                    )
            elif key == 'number_format':
                for decimals, cols in value.items():
                    col_defs_str.append(
                        f"{{ targets: {cols}, render: $.fn.dataTable.render.number( ',', '.', {decimals}, '&nbsp;', '&nbsp;' ) }},"
                    )
            elif key == 'string_format%':
                col_defs_str.append(
                    f"{{ targets: {value},  render: function (data, type, row) {{ return type === 'display' && data !== '' ? '&nbsp;' + data + '%&nbsp;' : data; }} }},"
                )
            elif key == 'string_format':
                col_defs_str.append(
                    f"{{ targets: {value},  render: function (data, type, row) {{ return type === 'display' && data !== '' ? '&nbsp;' + data + '&nbsp;' : data; }} }},")

        return  '\n            '.join(col_defs_str)
    
    @staticmethod
    def display(df, *args, **kwargs):
        params = DataFrameDisplay.display_validate_parameters(df, *args, **kwargs)

        if 'color_rules' in params:
            df = DataFrameDisplay.set_colors(df, *params['color_rules'])

        if "_totals_type" in df.columns and 'format_totals' in params and params['format_totals']:
            df = DataFrameDisplay.set_colors(df, dict(condition='_totals_type >= 3', style_code='italic'))

        all_cols = df.columns
        cols = [col for col in all_cols if col not in ('_rownum', '_totals_type', '_color_style')]
        dtypes = [dtype for dtype in df.dtypes if dtype[0] not in ('_rownum', '_totals_type', '_color_style')]
        has_rownum = '_rownum' in all_cols
        # has_colors = '_color_style' in all_cols
        nummeric_columns = [
            i for i, (col, dtype) in enumerate(dtypes)
            if Tools.check_data_type(dtype, 'num')
        ]
        pretty_headers = params['pretty_headers'] if 'pretty_headers' in params else False
        column_grouping = params['column_grouping'] if 'column_grouping' in params else True
        column_grouping_split_pattern = params['column_grouping_split_pattern'] if 'column_grouping_split_pattern' in params else '__'
        percentage_columns_pattern = params['percentage_columns_pattern'] if 'percentage_columns_pattern' in params else r'(_perc|%)$'

        # We add a row number to the dataframe to enable proper sorting and pagination in the datatable in javascript.
        # If sorting is requested, we do this the real way, with a rownum
        # if not requested but rownum is already present we use that
        # otherswise we just pick the first n rows and add a dummy rownum for the datatable
        if 'sort' in params:
            sort_by = DataFrameExtensions.transform_column_expressions(df, *params['sort'])
            df = df.withColumn('_rownum', F.row_number().over(W.orderBy(*sort_by)))
            df = df.filter(F.col('_rownum') <= params['n']).orderBy('_rownum')
        elif has_rownum:
            df = df.filter(F.col('_rownum') < params['n'] + 1).orderBy('_rownum')            
        else:
            # pick random rows and add a dummy rownum for the datatable. Note that if the dataframe is already 
            # sorted this will pick the top n rows
            df = df.limit(params['n']).withColumn('_rownum', F.monotonically_increasing_id())
            
        # rownum to last position and let datatables dor the sort
        df = df.selectExpr('* except(_rownum)', '_rownum')
        other_options = f"""order: [[{len(cols)}, 'asc']], ordering: true"""
        
        # collect the data
        df_collected, df_statistics = DataFrameDisplay.collect_data_and_stats(df, all_cols, cols, dtypes)
        columns_popup = str(list([
            html.escape(
                col + '---(' + dtype + ')'
                if len(dtype) <= 25
                else col + '---(' + dtype[0:22] + '...)'
            )
            for col, dtype in dtypes
        ]))

        if df_statistics['__total__']['width_with_header'] * 8 + 50 < 600:
            max_width = '600px'
        else:
            max_width = str(df_statistics['__total__']['width_with_header'] * 9 + 50) + 'px'  # rough estimate of width in pixels

        headers, header_length, uneven_columns = DataFrameDisplay.get_headers(cols, pretty_headers, column_grouping, column_grouping_split_pattern)
        page_length = params['page_length'] - header_length + 1
        # col_defs_grouped_columns = "{ targets: " + str(uneven_columns) + ",  className: 'grouped_column'}"
        _options = sorted([5, 50, page_length])
        _options = list(dict.fromkeys(_options))
        length_menu = str([[*_options, -1], [*_options, "All"]])

        if header_length > 1:
            # don't strip the rows, strip the columns
            other_options += ", stripeClasses: []"

        # Load template using relative path from this file's location
        with open(pathlib.Path(__file__).parent / "templates" / "dataframe_view_template.html", 'r', encoding='utf-8') as f:
            html_content = f.read()

        column_definitions = DataFrameDisplay.columns_definitions(cols, nummeric_columns, percentage_columns_pattern, df_statistics, uneven_columns)

        # create html
        html_content = html_content.replace('{generation_date}', datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        html_content = html_content.replace('{main_table}', DataFrameDisplay.data_to_html_table(df_collected, headers, df.columns))
        html_content = html_content.replace('{columns}', columns_popup)
        html_content = html_content.replace('{col_defs}', column_definitions)
        html_content = html_content.replace('{other_options}', other_options)
        html_content = html_content.replace('{max_width}', max_width)
        html_content = html_content.replace('{page_length}', str(page_length))
        html_content = html_content.replace('{length_menu}', length_menu)

        if 'file_path' in params:
            # save to file
            with open(params['file_path'], 'w', encoding='utf-8') as f:
                f.write(html_content)

        max_height = str(int(min(df_statistics['__total__']['rows'], page_length) * 25 + 178)) + 'px'
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
            return DataFrameTap._tap_end()