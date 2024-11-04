import os
import pickle
import webbrowser
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

from flask import Flask, render_template, request, session, redirect, url_for, send_file
from flask_wtf import FlaskForm
from flask_wtf.file import FileField
from wtforms import SubmitField, StringField, IntegerField, SelectField
from wtforms.validators import Optional

from lightgbm import LGBMClassifier
from io import BytesIO

from database_to_excel import SQLiteToFile
from excel_to_sql import FileToSQLite

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'

FILE_LIST = [
    'ANACAMARGE_SYNTHESE.xlsx',
    'CA BENCH REPORTING FACTORIE.pdf',
    'CA HT CAROLINE.pdf',
    'CA MARKET CAROLINE SUPER.pdf',
    'CASSE CAROLINE.xlsx',
]

download_service = SQLiteToFile()
file_service = FileToSQLite()


class UploadFileForm(FlaskForm):
    file1 = FileField(FILE_LIST[0])
    file2 = FileField(FILE_LIST[1])
    file3 = FileField(FILE_LIST[2])
    file4 = FileField(FILE_LIST[3])
    file5 = FileField(FILE_LIST[4])
    week = StringField('Week')
    submit = SubmitField("Upload File")


class UploadExtractionFileForm(FlaskForm):
    file1 = FileField("Extraction")
    week = StringField('Week')
    submit = SubmitField("Upload File")


class PredictionForm(FlaskForm):
    week = StringField('Week')
    dropdown = SelectField('Dropdown', choices=[(
        'Lindt', 'Lindt'), ("L'Oreal", "L'Oreal")])
    revenue = IntegerField("Mean_Revenue", validators=[Optional()])
    submit = SubmitField("Calculate Prediction")


def get_dates_from_week(year_week_str):
    year, week_num = map(int, year_week_str.split('-W'))
    start_of_week = datetime.strptime(
        f'{year}-W{week_num}-1', '%Y-W%W-%w').date()
    dates_in_week = [start_of_week + timedelta(days=i) for i in range(7)]
    return dates_in_week


@app.route('/', methods=['GET', 'POST'])
def home():
    try:
        success_list = session.get('success_list', [])
        session.pop('success_list')
        return render_template("home.html", success_list=success_list)
    except:
        return render_template("home.html")


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    form = UploadFileForm()
    success_list = []

    if form.validate_on_submit():
        if request.method == 'POST':
            selected_week = request.form.get('date')
            selected_market = request.form.get('market')
            print(selected_week)
        if request.files['file1'].filename.startswith('ANACAMARGE_SYNTHESE') and request.files['file1'].filename.endswith('.xlsx'):
            file1 = request.files['file1']
            file_service.process_anacamarge_synthese_xlsx(
                file1, selected_week, selected_market)
            success_list.append(request.files['file1'].filename)
        if request.files['file2'].filename == 'CA BENCH REPORTING FACTORIE.pdf':
            file2 = request.files['file2']
            file_service.process_ca_bench_reporting_factorie_pdf(
                file2, selected_week)
            success_list.append(request.files['file2'].filename)
        if request.files['file3'].filename == 'CA HT CAROLINE.pdf':
            file3 = request.files['file3']
            file_service.process_ca_ht_caroline_pdf(file3, selected_week)
            success_list.append(request.files['file3'].filename)
        if request.files['file4'].filename == 'CA MARKET CAROLINE SUPER.pdf':
            file4 = request.files['file4']
            file_service.process_ca_market_caroline_super_pdf(
                file4, selected_week)
            success_list.append(request.files['file4'].filename)
        if request.files['file5'].filename == 'CASSE CAROLINE.xlsx':
            file5 = request.files['file5']
            file_service.process_casse_caroline_xlsx(file5, selected_week)
            success_list.append(request.files['file5'].filename)

        session['success_list'] = success_list
        return redirect(url_for('home'))

    return render_template("upload.html", form=form, file_list=FILE_LIST)


@app.route('/upload_extraction', methods=['GET', 'POST'])
def upload_extraction():
    form = UploadExtractionFileForm()
    success_list = []

    if form.validate_on_submit():
        if request.method == 'POST':
            selected_week = request.form.get('date')
        if request.files['file1'].filename != None:
            file1 = request.files['file1']
            file_name = request.files['file1'].filename
            file_service.process_extraction_parametrable(
                file1, file_name, selected_week)
            success_list.append(request.files['file1'].filename)

        session['success_list'] = success_list
        return redirect(url_for('home'))

    return render_template("upload_extraction.html", form=form)


@app.route('/prediction', methods=['GET', 'POST'])
def prediction():
    form = PredictionForm()
    predictions = []
    predicted_df = []
    if form.validate_on_submit():
        selected_week = request.form.get('date')
        prediction_dates = get_dates_from_week(selected_week)
        company = form.dropdown.data
        mean_revenue = form.revenue.data
        if mean_revenue is not None:
            model = None
            if company == "Lindt":
                with open('MachineLearning/lgbm_model_mean.pkl', 'rb') as file:
                    model = pickle.load(file)
            else:
                with open('MachineLearning/lgbm_model_mean_loreal.pkl', 'rb') as file:
                    model = pickle.load(file)
            prediction_df = pd.DataFrame({
                "Year": [date.year for date in prediction_dates],
                "Month": [date.month for date in prediction_dates],
                "Day": [date.day for date in prediction_dates],
                "Day_of_week": [date.weekday() for date in prediction_dates],
                "Revenue Mean": [mean_revenue for i in range(len(prediction_dates))]
            })
            predictions = model.predict(prediction_df)
        else:
            model = None
            if company == "Lindt":
                with open('MachineLearning/lgbm_model.pkl', 'rb') as file:
                    model = pickle.load(file)
            else:
                with open('MachineLearning/lgbm_model_loreal.pkl', 'rb') as file:
                    model = pickle.load(file)
            prediction_df = pd.DataFrame({
                "Year": [date.year for date in prediction_dates],
                "Month": [date.month for date in prediction_dates],
                "Day": [date.day for date in prediction_dates],
                "Day_of_week": [date.weekday() for date in prediction_dates],
            })
            predictions = model.predict(prediction_df)
        if len(predictions) != 0:
            prediction_dates = np.append(prediction_dates, "Summe")
            predictions = np.append(predictions, sum(predictions))
            predicted_df = pd.DataFrame({
                'Date': prediction_dates,
                'Predicted Revenue': [round(prediction, 2) for prediction in predictions]
            })
            html_table = predicted_df.to_html(
                classes=["table", "mt-3", "table-hover"], index=False)
            html_table = html_table.replace(
                '<th>', '<th style="text-align: center;">')
            html_table = html_table.replace(
                '<tbody>', '<tbody style="text-align: center;">')
            return render_template("prediction.html", form=form, table=html_table)
    return render_template("prediction.html", form=form)


@app.route('/download', methods=['GET', 'POST'])
def download():
    table_names = download_service.get_table_names()

    if request.method == 'POST':
        selected_table = request.form['table']
        table_data = download_service.get_table_data(selected_table)
        csv_data = BytesIO()
        table_data.to_csv(csv_data, index=False, encoding="utf-8-sig")
        csv_data.seek(0)
        return send_file(csv_data,
                         as_attachment=True,
                         download_name=f'{selected_table}.csv',
                         mimetype='text/csv',
                         conditional=True)

    return render_template("download.html", table_names=table_names)


if __name__ == '__main__':

        # The reloader has not yet run - open the browser
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        webbrowser.open_new('http://127.0.0.1:5000/')

    app.run(debug=True)
