import pandas as pd
df = pd.read_csv('wages_by_education.csv') # Загрузка данных
df.head()

selected_columns = [
    'year',
    # Общие данные по образованию
    'less_than_hs', 'high_school', 'some_college', 'bachelors_degree', 'advanced_degree',
    # По расе
    'white_bachelors_degree', 'black_bachelors_degree', 'hispanic_bachelors_degree',
    # По полу
    'men_bachelors_degree', 'women_bachelors_degree',
    # Комбинация: раса + пол
    'white_men_bachelors_degree', 'white_women_bachelors_degree',
    'black_men_bachelors_degree', 'black_women_bachelors_degree',
    'hispanic_men_bachelors_degree', 'hispanic_women_bachelors_degree'
]

df_filtered = df[selected_columns] # Фильтрация данных
df_filtered.head() # Проверка
df_filtered = df_filtered[::-1] # Переворачиваем данные (от 1973 до 2022 года)
df_filtered.insert(0, 'id', range(1, len(df) + 1)) # Добавляем id
df_filtered.to_csv('wages_filtered.csv', index=False) # Экспортируем в файл csv