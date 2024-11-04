import sqlite3
import tabula
import re
import pandas as pd

from datetime import datetime


class FileToSQLite():

    def __init__(self) -> None:
        self.sqlite_db_path = "database.db"

    def clean_cell_values(self, cell):
        if pd.notna(cell):
            cell = str(cell).replace('%', '').replace(
                '(', '').replace(')', '').replace(' ', '').replace(',', '.')
            return float(cell)
        else:
            return cell

    def process_anacamarge_synthese_xlsx(self, excel_file, selected_week, selected_market):
        try:
            df = pd.read_excel(excel_file, header=[2, 3])
            df.rename(
                columns={
                    'Unnamed: 5': df.columns[4],
                    'Unnamed: 6': df.columns[4],
                    'Unnamed: 8': df.columns[7],
                    'Unnamed: 9': df.columns[7],
                    'Unnamed: 11': df.columns[10],
                    'Unnamed: 12': df.columns[10],
                }, inplace=True)
            column_names = [
                ' '.join(filter(pd.notna, col)) for col in df.columns]
            df.columns = [col.replace('.', '') for col in column_names]
            df.columns = df.columns.astype(str).str.replace('\n', ' ')

            df = df.drop([df.columns[0], df.columns[3]], axis=1)
            df.rename(
                columns={
                    'Unnamed: 1_level_0 Unnamed: 1_level_1': 'Id',
                    'Unnamed: 2_level_0 Unnamed: 2_level_1': 'Category'
                }, inplace=True)
            df = df.iloc[:-2]
            df['upload_date'] = pd.to_datetime(
                datetime.today().strftime('%d-%m-%Y'))
            df['report week'] = selected_week
            df['market'] = selected_market

            # multiply k€ by 1000 for visualization
            columns_to_multiply = [col for col in df.columns if 'k€' in col]
            df[columns_to_multiply] *= 1000
            df.columns = df.columns.str.replace(' (k€)', '')

            connection = sqlite3.connect(self.sqlite_db_path)
            df.to_sql(name="anacamarge_synthese", con=connection,
                      if_exists="append", index=False)

            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")

    def process_ca_bench_reporting_factorie_pdf(self, pdf_file, selected_week):
        try:
            df = tabula.read_pdf(pdf_file, pages='all',
                                 multiple_tables=False)[0]
            journee_value = df.iloc[0, 3].split(" ")[0]
            df = df.drop([0, 1, 2])
            df = df.drop(df.columns[-1], axis=1)
            df = df[df.iloc[:, 0] != "SURFACE DE VENTE"]
            df = df.dropna(subset=[df.columns[0]])
            df = df.reset_index(drop=True)
            column_names = [
                'SURFACE DE VENTE',
                f"{journee_value} CA TTC K€",
                f"{journee_value} CA TTC % Evol",
                f"{journee_value} Débits Nbre",
                f"{journee_value} Débits % Evol",
                f"{journee_value} Panier €",
                f"{journee_value} Panier % Evol",
                "Semaine à date CA TTC K€",
                "Semaine à date CA TTC % Evol",
                "Semaine à date Débits Nbre",
                "Semaine à date Débits % Evol",
                "Semaine à date Panier €",
                "Semaine à date Panier % Evol",
                "Actualisé Mois CA TTC K€",
                "Actualisé Mois CA TTC % Evol",
            ]
            df.columns = column_names
            df_cleaned = df.iloc[:, 1:].applymap(self.clean_cell_values)
            df = pd.concat([df.iloc[:, :1], df_cleaned], axis=1)
            df['upload_date'] = pd.to_datetime(
                datetime.today().strftime('%d-%m-%Y'))
            df['report week'] = selected_week
            columns_to_multiply = [col for col in df.columns if 'K€' in col]
            df[columns_to_multiply] *= 1000
            df.columns = df.columns.str.replace('K€', '€')
            connection = sqlite3.connect(self.sqlite_db_path)
            df.to_sql(name="ca_bench_reporting_factorie", con=connection,
                      if_exists="append", index=False)

            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")

    def process_ca_ht_caroline_pdf(self, pdf_file, selected_week):
        try:
            df = tabula.read_pdf(pdf_file,
                                 pages='all', multiple_tables=False)[0]
            df['Rayon'].fillna(method='ffill', inplace=True)
            df = df[df.iloc[:, 0] != "Rayon"]
            df.reset_index(drop=True)
            df['upload_date'] = pd.to_datetime(
                datetime.today().strftime('%d-%m-%Y'))
            df['report week'] = selected_week
            connection = sqlite3.connect(self.sqlite_db_path)
            df.to_sql(name="ca_ht_caroline", con=connection,
                      if_exists="append", index=False)

            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")

    def process_ca_market_caroline_super_pdf(self, pdf_file, selected_week):
        try:
            df = tabula.read_pdf(pdf_file, pages='all',
                                 multiple_tables=False)[0]
            df['upload_date'] = pd.to_datetime(
                datetime.today().strftime('%d-%m-%Y'))
            df['report week'] = selected_week
            connection = sqlite3.connect(self.sqlite_db_path)
            df.to_sql(name="ca_market_caroline_super", con=connection,
                      if_exists="append", index=False)

            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")

    def process_casse_caroline_xlsx(self, excel_file, selected_week):
        try:
            dfs = []
            sheet_names = pd.ExcelFile(excel_file).sheet_names

            for sheet_number in range(len(sheet_names)):
                sheet_name = f'Sheet{sheet_number+1}'
                df = pd.read_excel(excel_file, sheet_name, header=6)
                df = df.loc[:, ~df.columns.str.contains(
                    "Unnamed: 0|Unnamed: 1|Unnamed: 4|Unnamed: 6")]
                df.columns = [re.sub(r'\s+', ' ', col) for col in df.columns]
                df.rename(columns={'Unnamed: 2': "Index"}, inplace=True)
                df['sheet_name'] = sheet_name
                df['upload_date'] = pd.to_datetime(
                    datetime.today().strftime('%d-%m-%Y'), dayfirst=True)
                df['report week'] = selected_week
                dfs.append(df)

            result_df = pd.concat(dfs, ignore_index=True)

            connection = sqlite3.connect(self.sqlite_db_path)
            result_df.to_sql(name="casse_caroline", con=connection,
                             if_exists="append", index=False)

            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")

    def process_extraction_parametrable(self, input_file, file_name, selected_week):
        try:
            df = None
            if file_name.endswith(".csv"):
                df = pd.read_csv(input_file, sep=';', header=17)
                df = df.iloc[:, :30]
                df.replace(to_replace=r',', value='.',
                           regex=True, inplace=True)
                df.columns = df.columns.str.strip()
                df = df.drop(
                    columns=[col for col in df.columns if col.strip() == '' or col.strip() == '.1'])
                df = df[~(df['PAHT'].str.strip() == "") | df['PAHT'].isna()]
                columns_to_remove = ['PV Mag', 'Type Qté', 'Article Libellé Court', 'Type PA', 'SRP', 'Indicateur PVC', 'Type PV Mag', 'Indicateur PV Mag', 'TVA en %', 'Type Qté', 'PV Mag HT', 'Typologie', 'Libellé Unité de Besoin', 'Libellé UG','IFLS' ]

                for column in columns_to_remove:
                    if column in df.columns:
                        df = df.drop(column, axis=1)

                float_columns = ['PAHT', 'Quantité vendue *',
                 'Montant achat HT *', 'Montant vente TTC *', 'Marge en valeur',
                 'Marge en %', 'Stock en quantité']

                for column in float_columns:
                    if column in df.columns:
                        df[column] = df[column].replace("  ", 0)
                        try:
                            df[column] = df[column].astype(float)
                        except ValueError as e:
                            print(f"Error: {e}")


            elif file_name.endswith(".xlsx"):
                df = pd.read_excel(input_file, header=17)
                df.columns = df.columns.str.strip()
                df = df.dropna(axis=1, how='all')

                print(df)


            df['upload_date'] = pd.to_datetime(
                datetime.today().strftime('%d-%m-%Y'))
            year, week_number = map(int, selected_week.split('-W'))
            df['report week'] = week_number
            df['report year'] = year
            print(type(selected_week))
            connection = sqlite3.connect(self.sqlite_db_path)
            df.to_sql(name="extraction_parametrable", con=connection,
                      if_exists="append", index=False)
            connection.commit()
            connection.close()

        except Exception as e:
            print(f"Error: {e}")
