from .Tools import Tools
from IPython.display import HTML
import pandas as pd
import math
import os
from datetime import datetime
from pyspark.sql import DataFrame
from .settings import LION_TOOLS_COCKPIT_PATH, LION_TOOLS_TMP_PATH
from .DataFrameExtensions import DataFrameExtensions
import pyspark.sql.functions as F
import base64


class DataFrameExcel:
    
    @classmethod
    def examples(cls):
        Tools.display(HTML(Tools.load_file("dataframe_excel_examples.html", type='template')))

    @classmethod
    def get_column_format(cls, format):
        if format == 'euro0':
            new_format = '_(€ * #,##0_);_(€ * (#,##0);_(€ * "-"_);_(@_)'
        elif format == 'number0':
            new_format = '#,##0'
        elif format.startswith('euro'):
            zeros = '0' * int(format.replace('euro', ''))
            new_format = f'_(€ * #,##0.{zeros}_);_(€ * (#,##0.{zeros});_(€ * "-"??_);_(@_)'
        elif format.startswith('number'):
            zeros = '0' * int(format.replace('number', ''))
            new_format = f'#,##0.{zeros}'
        else:
            # assume the given format is actually an Excel format
            new_format = format

        return new_format
    
    @classmethod
    def get_color(cls, color):
        background_colors = {
            0: '#FFF0FF',
            1: '#F0FFF0',
            2: '#FFFFF0',
            3: '#F0FFFF',
            4: '#FFF0F0',
            5: '#F0F0FF',
            6: '#F0F0F0',
        }

        return background_colors[color % len(background_colors)] if isinstance(color, int) else color

    @classmethod
    def to_excel(
        cls,
        *dfs: list,
        name: str = None,
    ):
        
        name = name or "excel_export"
        path = str(LION_TOOLS_TMP_PATH.joinpath(f"{name}.xlsx"))
        path = path.replace('.xlsx.xlsx', '.xlsx')  # in case the user already added .xlsx in the name
        cls._write_excel(*dfs, path=path)

        cls.download_button(path=path)

    @classmethod
    def to_cockpit(
        cls,
        *dfs: list,
        name: str = None,
    ):
        name = name or "excel_export"
        name = "_lion_tools_tmp_" + name + "_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        path = str(LION_TOOLS_COCKPIT_PATH.joinpath(f"{name}.xlsx"))
        path = path.replace('.xlsx.xlsx', '.xlsx')  # in case the user already added .xlsx in the name
        cls._write_excel(*dfs, path=path)

    @classmethod
    def to_file(
        cls,
        *dfs: list,
        path: str,
    ):
        cls._write_excel(*dfs, path=path)

    @classmethod
    def _write_excel(
        cls,
        *dfs: list,
        path: str,
    ):
        
        # validate parameters
        dfs = list(dfs)

        if len(dfs) == 0:
            raise Exception("At least one dataframe must be provided.")
        
        assert all(isinstance(df, DataFrame) or isinstance(df, dict) for df in dfs), (
            '*dfs must be a list of dataframes and/or dictionaries.'
        )

        dfs = [df if isinstance(df, dict) else dict(df=df) for df in dfs]

        assert path.endswith('.xlsx'), "path must end with .xlsx"
    
        # do the magic, first create new subdirectories if needed
        dir = os.path.dirname(path)
        
        if not os.path.exists(dir):
            os.makedirs(dir)

        # delete the file to have the new file get a new modified data
        try:
            os.remove(path)
        except:
            pass

        # set the defaults per sheet
        sheets = []
        for i, df in enumerate(dfs):
            sheets.append(dict(
                df=df['df'],
                name=df.get('name', f'Sheet{i+1}'),
                column_options=df.get('column_options', None),
                autofilter=df.get('autofilter', True),
                freeze_first_row=df.get('freeze_first_row', True),
                index=df.get('index', False),
                autofit=df.get('autofit', True),
                border=df.get('border', 1),
                border_color=df.get('border_color', '#D3D3D3'),
                num_format=df.get('num_format', None),
            ))

        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        workbook  = writer.book

        for sheet in sheets:
            pd_df = sheet['df'].toPandas()
            pd_df.to_excel(
                writer,
                sheet_name=sheet['name'],
                index=sheet['index'],
                startrow=1,
                header=False,
            )

            worksheet = writer.sheets[sheet['name']]

            # calculate some values we are gonna use
            header_shift = 1 if sheet['index'] else 0
            max_row, max_col = pd_df.shape
            all_column_options = sheet['column_options']
            if all_column_options is None:
                all_column_options = cls.defaults_options(sheet['df'], return_or_print='return')

            num_columns = [
                col for col, dtype in sheet['df'].dtypes
                if Tools.check_data_type(dtype, 'num')
            ]

            # set autofilter
            if sheet['autofilter']:
                worksheet.autofilter(0, 0, max_row, max_col - 1)
            
            # set freeze
            if sheet['freeze_first_row']:
                worksheet.freeze_panes(1, 0)

            # set column options
            for colnum, col in enumerate(pd_df.columns):
                column_options = all_column_options[col] if col in all_column_options else dict()
                excel_format = dict()

                if 'width' in column_options and column_options['width']:
                    width = column_options['width']
                elif sheet['autofit']:
                    if (
                        'format' in column_options and column_options['format'] and 
                        ('number' in column_options['format'] or 'euro' in column_options['format'])
                    ):
                        # nummeric
                        decimals = int(column_options['format'].replace('number', '').replace('euro', ''))
                        width = pd_df[col].apply(lambda x: len(f"{math.floor(x) if pd.notna(x) else 0:,.0f}")).max()
                        width = width + decimals
                        width = width + 1 if decimals > 0 else width
                        width = width + 2 if 'euro' in column_options['format'] else width
                    else:
                        width = pd_df[col].apply(lambda x: len(str(x))).max()
                        width = 5 + width * 0.65

                    width = width + 1
                    width = max(width, 4)
                    width = min(width, 80)
                else:
                    width = None

                # For some options we have some customization
                if 'format' in column_options and column_options['format']:
                    excel_format['num_format'] = cls.get_column_format(column_options['format'])
                elif sheet['num_format'] and col in num_columns:
                    excel_format['num_format'] = cls.get_column_format(sheet['num_format'])

                if 'bg_color' in column_options and column_options['bg_color']:
                    excel_format['bg_color'] = cls.get_color(column_options['bg_color'])
                else:
                    excel_format['bg_color'] = 'white'

                # Non customized options
                for option in [
                    option for option in list(column_options.keys())
                    if option not in ['format', 'width', 'bg_color']
                ]:
                    excel_format[option] = column_options[option]

                # options at sheet level
                excel_format['border'] = sheet['border']
                excel_format['border_color'] = sheet['border_color']

                # implement the column options
                excel_format_workbook = workbook.add_format(excel_format)
                worksheet.set_column(colnum + header_shift, colnum + header_shift, width=width, cell_format=excel_format_workbook)

            # Write the column headers with the defined format.
            for col_num, value in enumerate(pd_df.columns.values):
                header_format = {
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'bottom',
                }
                # add bg_color if predefined
                if value in column_options and 'bg_color' in column_options[value] and column_options[value]['bg_color']:
                    header_format['bg_color'] = cls.get_color(column_options[value]['bg_color'])
                header_format_workbook = workbook.add_format(header_format)
                value = value.replace('_', ' ').capitalize()
                worksheet.write(0, col_num + header_shift, value, header_format_workbook)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()

    @classmethod
    def download_button(cls, path: str, title: str = None, display_or_return: str = 'display'):
        title = title or os.path.basename(path)

        with open(path, "rb") as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        
        html = Tools.load_file("download_file_template.html", type='template')
        html = html.replace("{title}", title)
        html = html.replace("{type}", 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        html = html.replace("{content}", content)

        if display_or_return == 'display':
            Tools.display(HTML(html))
        else:
            return html

    @classmethod
    def get_decimals(cls, df: DataFrame):
        dtypes = df.dtypes

        decimals = (
            df
            .selectExpr(*[
                f"substr(cast(round(abs(`{col}`) - floor(abs(`{col}`)), 8) as string), 3) as `{col}`" for col, dtype in dtypes
                if Tools.check_data_type(dtype, 'num_non_int')
            ])
            .selectExpr(*[
                f"max(coalesce(if(`{col}` = 0, 0, length(`{col}`)), 0)) as `{col}`" for col, dtype in dtypes
                if Tools.check_data_type(dtype, 'num_non_int')
            ])
        )

        return(decimals.collect()[0].asDict())

    
    @classmethod
    def defaults_options(
        cls, 
        df: DataFrame, 
        options: list[str] = ['format'], 
        exclude_strings: bool = False, 
        return_or_print: str = 'print',
    ):

        options = [options] if isinstance(options, str) else options
        decimals = cls.get_decimals(df)

        column_options = dict()
        for col, dtype in df.dtypes:
            if exclude_strings and dtype == 'string':
                continue

            column_options[col] = dict()
            for option in options:
                if option == 'format':
                    if col in decimals:
                        column_options[col][option] = f'number{decimals[col]}'
                    elif Tools.check_data_type(dtype, 'num_int'):
                        column_options[col][option] = 'number0'
                    else:
                        column_options[col][option] = None
                else:
                    column_options[col]['color'] = None

        if return_or_print == 'print':
            options = (
                f'column_options = ' + '{\n' +
                '\n'.join([f"    '{option}': {str(column_options[option])}, " for option in column_options.keys()]) +
                '\n}'
            )
            print(options)
        else:
            return column_options