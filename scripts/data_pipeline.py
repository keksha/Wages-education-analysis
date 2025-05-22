import csv
import pymysql
import schedule
import time

# Параметры подключения к базе данных
DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_NAME = 'labs'

# Папка для сохранения данных
CSV_FOLDER = '/home/student/Labs/C3U4/spooldir/'

# Запрос для получения всех строк в таблице
SQL_QUERY = 'SELECT * FROM wages_analysis'

# Функция для выполнения запроса и сохранения данных в CSV-файл
def fetch_data_to_csv():
    # Подключение к базе данных
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cursor = conn.cursor()
    cursor.execute(SQL_QUERY)
    # Получение данных из базы данных
    rows = cursor.fetchall()
    # Закрытие соединения
    cursor.close()
    conn.close()
    # Разбиение данных на части по 5%
    total_rows = len(rows)
    rows_limit = round(total_rows * 0.05)
    for i in range(0, total_rows, rows_limit):
        # Генерация имени файла
        filename = f"data{i//rows_limit}.csv"
        file_path = os.path.join(CSV_FOLDER, filename)
        # Сохранение данных в CSV-файл
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'year,less_than_hs', 'high_school,some_college', 'bachelors_degree',
                             'advanced_degree', 'white_bachelors_degree', 'black_bachelors_degree',
                             'hispanic_bachelors_degree', 'men_bachelors_degree', 'women_bachelors_degree',
                             'white_men_bachelors_degree', 'white_women_bachelors_degree', 'black_men_bachelors_degree',
                             'black_women_bachelors_degree', 'hispanic_men_bachelors_degree',
                             'hispanic_women_bachelors_degree'])
            for row in rows[i:i+rows_limit]:
                writer.writerow(row)
        print(f'Data fetched and saved to file {filename}')

# Задача для выполнения запроса и сохранения данных в CSV-файл
def job():
    print('Fetching data...')
    fetch_data_to_csv()

# Запуск задачи каждые 10 секунд
schedule.every(10).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)