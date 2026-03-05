class DataFrameExcel:
    @staticmethod
    def examples():
        examples = """
            to_excel creates an Excel worksheet from Spark Dataframes with some nice functionality such as:
                - Multiple sheets by creating a sheet per dataframe
                - Formating of values
                - Background colors of columns
                - Set column width to longest value (automatic)
                - Autofilter (automatic)

                Parameters:
                dfs: list of dataframes or dictionaries with more details. See examples for the
                     workings of this parameter.

                    format:
                    [
                        dict(
                            df=dataframe,
                            name=sheet name,
                            column_options=[
                                'column name': dict(
                                    width=1-...,
                                    format='format code, see below',
                                    bg_color='color name or #code',
                                    other options, see https://xlsxwriter.readthedocs.io/format.html#format
                                    for details
                                ),
                                ...
                            ],
                            autofilter=True/False,
                            index=True/False,
                            autofit=True/False,
                            freeze_first_row=True/False,
                            border=0-13,
                            border_color='color name or #code',
                            num_format='format code, see below, or Excel native format',
                        ),
                        dict(
                            ... -> additional entry per dataframe=sheet
                        ),
                    ]
                - file_name: name of the file. When filled, path must be empty. When filled the full path will
                be: /Workspace/Shared/excel_exports/{file_name.xlsx}. Must end with .xlsx
                - path: The full path of the file. When filled, file_name must be empty. Must end with .xlsx
                - show_path: Boolean to indicate if a message is printed to the console.

                Example 1: Export the dataframe df to file my_file.xlsx in the standard export directory
                        /Workspace/Shared/excel_exports/

                to_excel(df, 'my_file.xlsx')

                Example 2: Export to a subdirectory of the standard export directory.

                to_excel(df, 'mdleeuw/my_file.xlsx')

                Example 3: Export to a full path in the Workspace.

                to_excel(df, path='/Workspace/Teams/BestTeam/some_sub_sir/my_file.xlsx')

                Example 4: Set the color of some of the columns.

                to_excel(
                    {
                        'df': df,
                        'column_options': {
                            'article_id': {'bg_color': 'yellow'},
                            'article_desc': {'bg_color': '#FFF0F0'},
                        },
                    },
                    file_name='my_file.xlsx'
                )

                Example 5: Set format of all nummeric columns to have 0 decimals and thousands separator.

                to_excel(
                    {
                        'df': df,
                        'num_format': 'number0',
                    },
                    file_name='my_file.xlsx'
                )

                Example 6: Set format of columns to have thousands separator, one column with 0 decimals and one
                column with a euro sign and 2 decimals.

                to_excel(
                    {
                        'df': df,
                        'column_options': {
                            'jobber_end_qty_at_financial_cost_price_amt': {'format': 'number0'},
                            'jobber_end_qty_at_csp_amt': {'format': 'euro2'},
                        },
                    },
                    file_name='my_file.xlsx'
                )

                Example 7: Export 2 dataframes to 2 sheets in the same Excel file.

                to_excel([voorbeeld_df, voorbeeld2_df], file_name='my_file.xlsx')
                or indentical:
                to_excel([{'df': voorbeeld_df}, {'df': voorbeeld2_df}], file_name='my_file.xlsx')

                Example 8: 2 named sheets.

                to_excel([
                    {'df': voorbeeld_df, 'name': 'Revenue'},
                    {'df': voorbeeld2_df, 'name': 'Inventory'}
                ], file_name='my_file.xlsx')

                Example 9, first sheet with options, second sheet without options.

                to_excel([
                    {
                        'df': voorbeeld_df, 'name': 'Revenue', 'column_options': {
                            'revenue_amount': {'bg_color': 'yellow', 'format': 'euro0'},
                            'quantity': {'bg_color': '#FFF0F0'},
                        }
                    },
                    voorbeeld2_df,
                ], file_name='my_file.xlsx')
        """


    def to_excel(
        *dfs: list,
    ):

        from pyspark.sql import DataFrame
        import pandas as pd
        import math
        import os

        def get_column_format(format):
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

        def get_color(color):
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

        assert bool(file_name) != bool(path), 'file_name or path must be filled, not both.'
        path = path if path else f'/Workspace/Shared/excel_exports/{file_name}'
        file_type = path.split('.')[-1]
        assert file_type == 'xlsx', (
            "file extention must be 'xlsx'.")

        dfs = dfs if isinstance(dfs, list) else [dfs]
        assert all(isinstance(df, DataFrame) or isinstance(df, dict) for df in dfs), (
            'dfs must be a list of dataframes or dictionaries.'
        )
        dfs = [df if isinstance(df, dict) else dict(df=df) for df in dfs]

        # create new subdirectories if needed
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
                column_options=df.get('column_options', dict()),
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
            with tprint(f'Creating sheet {sheet["name"]}'):
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
                num_columns = [
                    col for col, dtype in sheet['df'].dtypes
                    if dtype in ('int', 'integer', 'double', 'bigint') or dtype.startswith('decimal')
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
                        if 'format' in column_options and column_options['format'] and ('number' in column_options['format'] or 'euro' in column_options['format']):
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
                        excel_format['num_format'] = get_column_format(column_options['format'])
                    elif sheet['num_format'] and col in num_columns:
                        excel_format['num_format'] = get_column_format(sheet['num_format'])

                    if 'bg_color' in column_options and column_options['bg_color']:
                        excel_format['bg_color'] = get_color(column_options['bg_color'])
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
                        header_format['bg_color'] = get_color(column_options[value]['bg_color'])
                    header_format_workbook = workbook.add_format(header_format)
                    value = value.replace('_', ' ').capitalize()
                    worksheet.write(0, col_num + header_shift, value, header_format_workbook)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()