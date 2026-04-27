import pathlib
import decimal
import html
from pprint import pprint
from sys import prefix
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
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
import math

class DataFrameDisplay():

    # all defaults for the display and disply to cockpit functions are ajustable
    # the are defined here to be overwritten by the user
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

    defaults = dict(
        name = None,                 # name of the display, used for the tab name in the cockpit
        passthrough = False,         # should the dataframe be returned for chaining after display?
        compact = 0,                 # 1-3, should the display be compacted to make row less wide?
        add_time_to_name = False,    # should time be added to the name of the display??
        n = 1001,                    # number of rows tot diplay
        p = 15,                      # number of rows per page
        file_path = None,            # save the display to the file path
        sort = None,                 # column(s) to sort by before display, (list of) string or integers
        max_table_bytes = 200000,    # 500000, the maximum size of the table in bytes that will be displayed
        lazy = True,                 # should cockpit displays be lazy?
        color_rules = [],            # list of dicts with rules to color the cells
        pretty_headers = False,      # should the headers be prettified?
        format_totals = True,        # should the totals rows be formatted with a different style to make them recognizable?
        column_grouping = True,      # should columns be grouped when they have a common prefix separated
        column_grouping_split_pattern = '__',       # pattern to split column names into column groups
        percentage_columns_pattern = r'(_perc|%)$', # regex pattern to identify percentage columns for proper formatting
        display = True,              # whether to display the table (set to False for debugging)
    )

    new_line_placeholder = '___NEW_LINE___'

    @staticmethod
    def set_defaults(**kwargs):
        for key, value in kwargs.items():
            if key not in DataFrameDisplay.defaults:
                raise ValueError(f"Invalid default option: {key}")
            DataFrameDisplay.defaults[key] = value

    @staticmethod
    def display(
        df: DataFrame, 
        passthrough: bool = None,
        compact: int = None,
        n: int = None,
        p: int = None,
        file_path: str = None,
        sort: list = None,
        color_rules: list[dict] = None,
        pretty_headers: bool = None,
        format_totals: bool = None,
        column_grouping: bool = None,
        column_grouping_split_pattern: str = None,
        percentage_columns_pattern: str = None,
        display: bool = None,
     ):
        
        DataFrameDisplay(
            df=df, 
            passthrough=passthrough,
            compact=compact,
            n=n,
            p=p,
            file_path=file_path,
            sort=sort,
            color_rules=color_rules,
            pretty_headers=pretty_headers,
            format_totals=format_totals,
            column_grouping=column_grouping,
            column_grouping_split_pattern=column_grouping_split_pattern,
            percentage_columns_pattern=percentage_columns_pattern,
            display=display,
        )

    @staticmethod
    def set_colors(df, *color_rules: dict):
        if len(color_rules) == 0:
            raise ValueError("At least one color rule must be provided")
        
        if isinstance(color_rules[0], list):
            raise ValueError("Color rules should be provided as separate arguments, not as a list.")
        
        assert all(isinstance(rule, dict) for rule in color_rules), "color_rules must be a list of dictionaries"
        allowed_keys = {'column', 'columns', 'color_code', 'style_code', 'condition'}
        all_cols = [col for col in df.columns]
        cols = [col for col in all_cols if col not in ('_totals_type', '_color_style')]

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
            # step 0: create column _color_style_existing to be able to keep already defined colors/styles
            # when multiple set_colors are used in the same chain
            .withColumn(
                '_color_style_existing',
                F.col('_color_style')
                if '_color_style' in all_cols
                else F.array().cast('array<struct<column:string,color_code:string,style_code:string>>')
            )
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
            # step 3, if no color is set and there is an existing color/style keep it, otherwise set to null
            .withColumns({
                f"_{col}_color_code": 
                    F.when(
                        F.col(f"_{col}_color_code").isNull() & (F.size(F.col('_color_style_existing')) > 0), 
                        F.expr(f"try_element_at(filter(_color_style_existing, x -> x.column = '{col}' and x.color_code is not null), 1).color_code")
                    ).otherwise(F.col(f"_{col}_color_code"))
                for col in cols
            })
            .withColumns({
                f"_{col}_style_code": 
                    F.when(
                        F.col(f"_{col}_style_code").isNull() & (F.size(F.col('_color_style_existing')) > 0), 
                        F.expr(f"try_element_at(filter(_color_style_existing, x -> x.column = '{col}' and x.style_code is not null), 1).style_code")
                    ).otherwise(F.col(f"_{col}_style_code"))
                for col in cols
            })
            # step 4, compact the color/styles to the single column _color_style
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
                *[f"_{col}_style_code" for col in cols],
                '_color_style_existing',
            )
        )

    def __init__(
        self,
        df: DataFrame,
        passthrough: bool = None,
        compact: int = None,
        n: int = None,
        p: int = None,
        file_path: str = None,
        sort: list = None,
        max_table_bytes: int = None,
        color_rules: list[dict] = None,
        pretty_headers: bool = None,
        format_totals: bool = None,
        column_grouping: bool = None,
        column_grouping_split_pattern: str = None,
        percentage_columns_pattern: str = None,
        display: bool = None,
    ):

        # set instance variables and apply defaults from class defaults
        self.df = df
        self.passthrough = passthrough if passthrough is not None else DataFrameDisplay.defaults['passthrough']
        self.compact = compact if compact is not None else DataFrameDisplay.defaults['compact']
        self.n = n if n is not None else DataFrameDisplay.defaults['n']
        self.p = p if p is not None else DataFrameDisplay.defaults['p']
        self.file_path = file_path if file_path is not None else DataFrameDisplay.defaults['file_path']
        self.sort = sort if sort is not None else DataFrameDisplay.defaults['sort']
        self.max_table_bytes = max_table_bytes if max_table_bytes is not None else DataFrameDisplay.defaults['max_table_bytes']
        self.color_rules = color_rules if color_rules is not None else DataFrameDisplay.defaults['color_rules']
        self.pretty_headers = pretty_headers if pretty_headers is not None else DataFrameDisplay.defaults['pretty_headers']
        self.format_totals = format_totals if format_totals is not None else DataFrameDisplay.defaults['format_totals']
        self.column_grouping = column_grouping if column_grouping is not None else DataFrameDisplay.defaults['column_grouping']
        self.column_grouping_split_pattern = column_grouping_split_pattern if column_grouping_split_pattern is not None else DataFrameDisplay.defaults['column_grouping_split_pattern']
        self.percentage_columns_pattern = percentage_columns_pattern if percentage_columns_pattern is not None else DataFrameDisplay.defaults['percentage_columns_pattern']
        self.display = display if display is not None else DataFrameDisplay.defaults['display']

        # allow overloading
        self.sort = [self.sort] if self.sort and not isinstance(self.sort, list) else self.sort
        self.color_rules = [self.color_rules] if self.color_rules and not isinstance(self.color_rules, list) else (
            self.color_rules)

        # validate
        assert 'pyspark.sql' in str(type(self.df)) or 'DataFrame' in str(type(self.df))
        assert isinstance(self.passthrough, bool)
        assert isinstance(self.compact, int) and self.compact in (0, 1)
        assert isinstance(self.n, int) and self.n > 0 and self.n <= 100000
        assert isinstance(self.p, int) and self.p > 0 and self.p <= 100000
        assert isinstance(self.file_path, str) or self.file_path is None
        assert isinstance(self.sort, list) or self.sort is None
        assert isinstance(self.display, bool)
        assert isinstance(self.max_table_bytes, int) and self.max_table_bytes > 0 and self.max_table_bytes <= 10000000
        assert isinstance(self.color_rules, list)
        assert isinstance(self.pretty_headers, bool)
        assert isinstance(self.format_totals, bool)
        assert isinstance(self.column_grouping, bool)
        assert isinstance(self.column_grouping_split_pattern, str)
        assert isinstance(self.percentage_columns_pattern, str)

        if self.color_rules:
            assert all(isinstance(rule, dict) for rule in self.color_rules)
            self.df = DataFrameDisplay.set_colors(self.df, *self.color_rules)

        if self.format_totals and "_totals_type" in self.df.columns:
            self.df = DataFrameDisplay.set_colors(
                self.df, 
                dict(condition='_totals_type >= 3', style_code='italic')
            )

        self.prepare_data_with_rownum()
        self.gather_column_information()
        self.collect_data_and_stats()
        self.further_limit_data_by_table_bytes()
        self.set_columns_popup()
        self.set_headers()
        self.set_length_and_width()
        self.set_other_options()
        self.set_column_definitions()
        self.compact_headers()
        self.data_to_html_table()
        self.apply_to_template()

        if self.file_path:
            self.save_to_file()

        if self.display:
            self._display()

        if self.passthrough:
            return self.df 
        elif DataFrameTap.tapped and DataFrameTap.tapped['end_on_display']:
            return DataFrameTap._tap_end()

    def _display(self):
        self.put_in_iframe()
        display(display_HTML(self.iframe_html))

    def gather_column_information(self):
        # define some variables that are needed down the road
        self.all_dtypes = self.df.dtypes
        self.all_cols = [dtype[0] for dtype in self.all_dtypes]

        self.dtypes = [
            dtype for dtype in self.all_dtypes
            if dtype[0] not in ('_rownum', '_totals_type', '_color_style')
        ]
        self.cols = [dtype[0] for dtype in self.dtypes]
        
        self.nummeric_columns_indexes = [
            i for i, (col, dtype) in enumerate(self.dtypes)
            if Tools.check_data_type(dtype, 'num')
        ]
        self.nummeric_columns = [
            col for i, (col, dtype) in enumerate(self.dtypes)
            if Tools.check_data_type(dtype, 'num')
        ]
        self.integer_columns = [
            col for i, (col, dtype) in enumerate(self.dtypes)
            if Tools.check_data_type(dtype, 'num_int')
        ]
        
        self.table_cols = [
            col for col in self.all_cols
            if col not in ('_totals_type', '_color_style')
        ]
        self.table_dtypes = [
            dtype for dtype in self.all_dtypes
            if dtype[0] in self.table_cols
        ]

    def collect_data_and_stats(self):  
        self.df_statistics = {
            col: {
                'type': dtype, 
                'length': 1, 
                'total': 0, 
                'decimals': 0,
                'max_int_length': 0,
                'header_length': len(col)
            }
            for col, dtype in self.table_dtypes
        }

        self.df_collected = self.df.collect()

        if '_totals_type' in self.all_cols:
            self.remove_unnecessary_sub_totals()

        for row in self.df_collected:
            for col in self.table_cols:
                value = row[col]
                if value is not None:
                    length = len(str(value))
                    self.df_statistics[col]['length'] = max(self.df_statistics[col]['length'], length)
                    self.df_statistics[col]['total'] = self.df_statistics[col]['total'] + length
                    if col in self.nummeric_columns:
                         self.df_statistics[col]['max_int_length'] = max(
                             self.df_statistics[col]['max_int_length'], 
                             len(str(int(value)))
                         )
                    if (

                        col in self.nummeric_columns and 
                        col not in self.integer_columns and 
                        str(value).split('.')[-1] != '0'
                    ):
                        self.df_statistics[col]['decimals'] = max(
                            self.df_statistics[col]['decimals'], 
                            len(str(value).split('.')[-1])
                        )

        self.df_statistics['__total__'] = {
            'rows': len(self.df_collected), 
            'columns': len(self.table_cols), 
            'width': sum([self.df_statistics[col]['length'] for col in self.table_cols]),
            'avg_width': sum([self.df_statistics[col]['total'] for col in self.table_cols]) / len(self.df_collected) if len(self.df_collected) > 0 else 0,
            'width_with_header': sum([max(self.df_statistics[col]['length'], self.df_statistics[col]['header_length']) for col in self.table_cols]),
            'size_limit': False,
        }

    def further_limit_data_by_table_bytes(self):
        # if the total (byte) size of the data is large we limited the number of rows to avoid browser performance issues
        if self.df_statistics['__total__']['avg_width'] * self.df_statistics['__total__']['rows'] > self.max_table_bytes and len(self.df_collected) > 1:
            new_n = int(self.max_table_bytes / self.df_statistics['__total__']['avg_width'])
            self.df_collected = self.df_collected[:new_n]
            self.df_statistics['__total__']['size_limit'] = True
            self.df_statistics['__total__']['rows'] = new_n

    def prepare_data_with_rownum(self):
        # We add a row number to the dataframe to enable proper sorting and pagination in the datatable in javascript.
        # If sorting is requested, we do sort and get a monotonically increasing id as rownum
        # if not requested but rownum is already present we use that
        # otherswise we just pick the first n rows and add a dummy rownum for DataTable.
        if self.sort:
            sort_by = DataFrameExtensions.transform_column_expressions(self.df, *self.sort)
            self.df = self.df.orderBy(sort_by).withColumn('_rownum', F.monotonically_increasing_id())
            self.df = self.df.filter(F.col('_rownum') <= self.n).orderBy('_rownum')
        elif '_rownum' in self.df.columns:
            self.df = self.df.filter(F.col('_rownum') < self.n + 1).orderBy('_rownum')            
        else:
            # pick random rows and add a dummy rownum for DataTable. Note that if the dataframe is already 
            # sorted this will pick the top n rows
            self.df = self.df.limit(self.n).withColumn('_rownum', F.monotonically_increasing_id())

        # rownum to last position
        self.df = self.df.selectExpr('* except(_rownum)', '_rownum')

    def set_columns_popup(self):
        # collect the data
        self.columns_popup = str(list([
            html.escape(
                col + '---(' + dtype + ')'
                if len(dtype) <= 25
                else col + '---(' + dtype[0:22] + '...)'
            )
            for col, dtype in self.dtypes
        ]))

    def set_length_and_width(self):
        if self.df_statistics['__total__']['width_with_header'] * 8 + 50 < 600:
            self.max_width = '600px'
        else:
            self.max_width = str(self.df_statistics['__total__']['width_with_header'] * 9 + 50) + 'px'  # rough estimate of width in pixels

        self.p = self.p - self.header_length + 1
    
    def set_other_options(self):
        self.other_options = f"""order: [[{len(self.cols)}, 'asc']], ordering: true"""
        _options = sorted([5, 50, self.p])
        _options = list(dict.fromkeys(_options))
        self.length_menu = str([[*_options, -1], [*_options, "All"]])

        if self.header_length > 1:
            # don't strip the rows, strip the columns
            self.other_options += ", stripeClasses: []"

    def apply_to_template(self):
        # Load template using relative path from this file's location
        with open(pathlib.Path(__file__).parent / "templates" / "dataframe_view_template.html", 'r', encoding='utf-8') as f:
            html_content = f.read()

        # create html
        html_content = html_content.replace('{generation_date}', datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        html_content = html_content.replace('{main_table}', self.html_table)
        html_content = html_content.replace('{columns}', self.columns_popup)
        html_content = html_content.replace('{col_defs}', self.column_definitions)
        html_content = html_content.replace('{other_options}', self.other_options)
        html_content = html_content.replace('{max_width}', self.max_width)
        html_content = html_content.replace('{page_length}', str(self.p))
        html_content = html_content.replace('{length_menu}', self.length_menu)
        
        self.html_content = html_content

    def save_to_file(self):
        # save to file
        with open(self.file_path, 'w', encoding='utf-8') as f:
            f.write(self.html_content)

    def put_in_iframe(self):
        max_height = str(int(min(self.df_statistics['__total__']['rows'], self.p) * 25 + 178)) + 'px'
        
        # Wrap in an iframe with srcdoc to enable proper JavaScript execution
        self.iframe_html = f"""
            <iframe srcdoc='{self.html_content.replace("'", "&apos;")}'
                    width='99.9%' 
                    height='{max_height}px'
                    margin='0'
                    frameborder='0'
                    sandbox='allow-scripts allow-same-origin'
                    style='border: 1px solid #ddd; overflow-y: hidden; overflow-x: auto; display: block;'>
            </iframe>
        """       

    @staticmethod
    def split_near_middle(text):
        delimiters = {' ', '_'}
        valid_indices = [i for i, char in enumerate(text) if char in delimiters]
        if not valid_indices:
            return text, ""
        mid_point = len(text) / 2.0
        best_index = min(valid_indices, key=lambda x: abs(x - mid_point))
        
        return text[:best_index], text[best_index + 1:]

    def compact_headers(self):
        if self.compact == 1:
            # last bottom line of the headers will be wrapped into 2 lines when the
            # header is longer then the maximum width of the data in that column.
            bhr = len(self.headers) - 1 # bottom header rows

            for i, (col, dtype) in enumerate(self.dtypes):
                header = self.headers[bhr][i][1]
                if len(header) > self.df_statistics[col].get('display_length', 0):
                    splitted = self.split_near_middle(header)
                    new_header = self.new_line_placeholder.join(splitted) if splitted[1] != '' else header
                    self.headers[bhr][i][1] = new_header

    def escape(self,text):
        return html.escape(text).replace(self.new_line_placeholder, '<br>')
        
    def data_to_html_table(self):
        # note we don't use tabulate here as we need to build the table body with additional functionality

        headers_ext = [
            [
                i,
                'single' if i==0 and len(self.headers) == 1
                else 'last' if i == len(self.headers) - 1
                else 'non_last', 
                header_row
            ]
            for i, header_row in enumerate(self.headers)
        ]

        # table header
        table_header = "\n                    ".join(
            '<tr>' + 
                ''.join([
                    f'<th>{self.escape(str(header))}</th>' if header_type in ('last', 'single')
                    else f'<th>{self.escape(str(header))}</th>' if colspan == 1 and i % 2 == 0
                    else f'<th class="grouped_column">{self.escape(str(header))}</th>' if colspan == 1 and i % 2 == 1
                    else f'<th colspan="{colspan}">{self.escape(str(header))}</th>' if i % 2 == 0
                    else f'<th colspan="{colspan}" class="grouped_column">{self.escape(str(header))}</th>'
                    for i, (colspan, header) in enumerate(header_row)
                ]) +
            '</tr>'
            for i, header_type, header_row in headers_ext    
        )

        # table body
        table_body = ''
        for row in self.df_collected:
            table_body += '<tr>'
            for col in self.table_cols:
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
                value = self.cast_to_expandable_html(row[col])
                if style_str != '':
                    table_body += f'<td class="{style_str.strip()}">{value}</td>'
                else:
                    table_body += f'<td>{value}</td>'
            table_body += '</tr>\n'
                

        # bring it together
        self.html_table = f"""
            <table id="mainTable" class="display" style="width:100%">
                <thead>
                    {table_header}
                </thead>
                <tbody>
                    {table_body}
                </tbody>
            </table>
        """
    
    def cast_to_expandable_html(self, data, add_quotes_when_needed=False, preview_prefix=None, preview_postfix=None):
        if isinstance(data, Row):
            # Convert Row to a dictionary and handle it as a dict
            return self.cast_to_expandable_html(
                data.asDict(), 
                add_quotes_when_needed=True,
                # preview_prefix='Row(', 
                # preview_postfix=')'
            )

        # Handle Lists
        elif isinstance(data, list):
            # Create a single-line preview of the list
            preview = ", ".join(
                self.to_string(x, add_quotes_when_needed=True)
                for x in data
            )

            if (
                len(data) > 0 and 
                (isinstance(data[0], Row) or isinstance(data[0], dict) or isinstance(data[0], list))
            ):
                expanded_items = [
                    self.cast_to_expandable_html(item, add_quotes_when_needed=True)
                    for item in data
                ]
                multi_line = "".join(expanded_items)
            else:
                expanded_items = [
                    "- " + self.cast_to_expandable_html(item, add_quotes_when_needed=True)
                    for item in data
                ]
                multi_line = "<br>".join(expanded_items)
            
            return self.expandable_html(preview, multi_line, preview_prefix='[', preview_postfix=']')


        # Handle Dictionaries (Optional, but useful for complex data)
        elif isinstance(data, dict):
            preview = ", ".join(f"{k}: {self.to_string(v, add_quotes_when_needed=True)}" for k, v in data.items())
            expanded_items = [f"<b>{k}:</b> {self.cast_to_expandable_html(v, add_quotes_when_needed=True)}" for k, v in data.items()]
            multi_line = "<br>".join(expanded_items)

            return self.expandable_html(
                preview, 
                multi_line, 
                preview_prefix=preview_prefix if preview_prefix else '{', 
                preview_postfix=preview_postfix if preview_postfix else '}'
            )
            
        # Handle basic data types (strings, ints, floats, etc.)
        else:
            return self.to_string(data, add_quotes_when_needed=add_quotes_when_needed)
        
    def to_string(self, value, add_quotes_when_needed=False):
        if add_quotes_when_needed and isinstance(value, str):
            return f"'{html.escape(value)}'"
        elif add_quotes_when_needed and value is None:
            return 'null'
        elif value is None:
            return ''
        elif (
            self.compact > 0 and isinstance(value, str) and len(value) == 32 and 
                all(c in '0123456789abcdefABCDEF' for c in value)
            ):
            value = html.escape(value)
            return value[0:5] + "...." + value[-5:]
        else:
            return html.escape(str(value))

    def expandable_html(self, preview, multi_line, preview_prefix, preview_postfix, max_preview_length=60):
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

    def remove_unnecessary_sub_totals(self):
        # Unnecessary sub-totals are sub totals where there only is one record feeding the sub total.
        group_record_count = 0
        df_collected_new = []
        df_collected_len =len(self.df_collected)

        for rownum, row in enumerate(self.df_collected):
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
                    ((rownum + 2) < df_collected_len and self.df_collected[rownum + 2]['_totals_type'] > 2) or
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

        self.df_collected = df_collected_new

    def set_headers(self):
        # If column grouping is enabled we add an additional row in the header containing the group name.
        # The group name is derived from the column name by taking the part before the first occurrence of a split pattern. 
        # Note that the page length needs to be corrected to account for multi row headers.
        # Pretty headers are basically just a cosmetic change to make the headers more readable by replacing underscores with
        # spaces and capitalizing words.

        self.header_length = max([
            len(col.split(self.column_grouping_split_pattern))
            for col in self.cols
        ])

        if self.column_grouping and self.header_length > 1:
            split_cols = [col.split(self.column_grouping_split_pattern) for col in self.cols]
            split_cols = [
                split_col if not self.pretty_headers else [split_col_.replace('_', ' ').title() for split_col_ in split_col]
                for split_col in split_cols
            ]
            # pad the arrays with empty string
            split_cols = [[' '] * (self.header_length - len(split_col)) + split_col for split_col in split_cols]
            # transpose the matrix
            header_rows = list(map(list, zip(*split_cols)))
            # make single titles with count of the number columns the title must span
            self.headers = [
                [
                    [len(list(group)), label]
                    for label, group in groupby(header_row)
                ] 
                for header_row in header_rows
            ]
            self.uneven_columns = []
            i = 0
            for j, col in enumerate(self.headers[-2]):
                if j % 2 == 1:
                    for k in range(col[0]):
                        self.uneven_columns.append(i + k)
                i += col[0]

        else:
            # make just the single row
            self.header_length = 1
            self.uneven_columns = []
            self.headers = [[
                [1, col] if not self.pretty_headers else [1, col.replace('_', ' ').title()]
                for col in self.cols
            ]]

    @staticmethod
    def count_separators(max_int_length):
        # Convert to string and strip the negative sign if present
        return (max_int_length - 1) // 3

    def set_column_definitions(self):
        cols_defs = {}
        cols_defs['rownum'] = [len(self.cols)]
        cols_defs['alignment_right'] = self.nummeric_columns_indexes + [len(self.cols)]
        cols_defs['grouped_columns'] = self.uneven_columns

        for i, col in enumerate(self.cols):
            if i in self.nummeric_columns_indexes and re.search(self.percentage_columns_pattern, col):
                # nummeric + %
                decimals = self.df_statistics[col]['decimals']
                cols_defs.setdefault('number_format%', {})
                cols_defs['number_format%'].setdefault(decimals, [])
                cols_defs['number_format%'][decimals].append(i)
                self.df_statistics[col]['display_length'] = (
                    self.df_statistics[col]['max_int_length'] + # digits before the decimal point
                    DataFrameDisplay.count_separators(self.df_statistics[col]['max_int_length']) +  # separators
                    self.df_statistics[col]['decimals'] +  # decimals
                    2  # percentage sign + decimal point
                )
            elif i in self.nummeric_columns_indexes:
                # nummeric
                decimals = self.df_statistics[col]['decimals']
                cols_defs.setdefault('number_format', {})
                cols_defs['number_format'].setdefault(decimals, [])
                cols_defs['number_format'][decimals].append(i)
                self.df_statistics[col]['display_length'] = (
                    self.df_statistics[col]['max_int_length'] + # digits before the decimal point
                    DataFrameDisplay.count_separators(self.df_statistics[col]['max_int_length']) +  # separators
                    self.df_statistics[col]['decimals'] +  # decimals
                    (1 if self.df_statistics[col]['decimals'] > 0 else 0)  # decimal point
                )
            elif re.search(self.percentage_columns_pattern, col):
                # not nummeric + %
                cols_defs.setdefault('string_format%', [])
                cols_defs['string_format%'].append(i)
                self.df_statistics[col]['display_length'] = self.df_statistics[col]['length'] + 1
            else:
                # not nummeric
                cols_defs.setdefault('string_format', [])
                cols_defs['string_format'].append(i)
                self.df_statistics[col]['display_length'] = self.df_statistics[col]['length']

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

        self.column_definitions = '\n            '.join(col_defs_str)
    
