import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10-2.12:3.1.1 pyspark-shell'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppName") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("SHOW TABLES").show()
df = spark.sql("SELECT * FROM hive_wages") # Импорт данных из Hive

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col

# Влияние образования на зарплату
spark.sql("SELECT \
    ROUND(AVG(less_than_hs),2) AS avg_less_hs, ROUND(AVG(high_school),2) AS avg_hs, \
    ROUND(AVG(some_college),2) AS avg_some_college, ROUND(AVG(bachelors_degree),2) AS avg_bachelors, \
    ROUND(AVG(advanced_degree),2) AS avg_advanced \
FROM hive_wages").show()

plt.figure(figsize=(10, 6))
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

for i, col in enumerate(['less_than_hs', 'high_school', 'some_college', 'bachelors_degree', 'advanced_degree']):
    sns.lineplot(data=df_pd, x='year', y=col,
                color=colors[i], linewidth=2.5,
                label=col.replace('_', ' ').title())

plt.title('Зарплата по уровню образования', fontsize=14)
plt.ylabel('Зарплата ($)', fontsize=12)
plt.xlabel('Год', fontsize=12)
plt.legend(title='Уровень образования')
plt.grid(True, linestyle='--', alpha=0.3)
plt.tight_layout()
plt.show()

# Разница в зарплатах по расе
race_df = spark.sql("SELECT \
    ROUND(AVG(white_bachelors_degree),2) AS avg_white, \
    ROUND(AVG(black_bachelors_degree),2) AS avg_black, \
    ROUND(AVG(hispanic_bachelors_degree),2) AS avg_hispanic, \
    ROUND(AVG(white_bachelors_degree - black_bachelors_degree),2) AS white_black_gap, \
    ROUND(AVG(white_bachelors_degree - hispanic_bachelors_degree),2) AS white_hispanic_gap \
FROM hive_wages")
race_df.show()

race_pd = race_df.toPandas()
plt.figure(figsize=(8, 6))
race_melted = race_pd.melt(value_vars=['avg_white', 'avg_black', 'avg_hispanic'],
                          var_name='race', value_name='salary')
ax = sns.barplot(x='race', y='salary', data=race_melted)

plt.title('Зарплаты среди бакалавров по расе')
plt.ylabel('Зарплата ($)')
plt.xlabel('')
plt.xticks([0,1,2], ['Белые', 'Темнокожие', 'Латиноамериканцы'])
plt.show()

# Гендерный разрыв
gender_df = spark.sql("SELECT \
    ROUND(AVG(men_bachelors_degree),2) AS avg_men, \
    ROUND(AVG(women_bachelors_degree),2) AS avg_women, \
    ROUND(AVG(men_bachelors_degree - women_bachelors_degree),2) AS gender_gap \
FROM hive_wages")
gender_df.show()

gender_pd = gender_df.toPandas()
plt.figure(figsize=(7, 5))
gender_melted = gender_pd.melt(value_vars=['avg_men', 'avg_women'],
                              var_name='gender', value_name='salary')
ax = sns.barplot(x='gender', y='salary', data=gender_melted)
gender_pd['percent_gap'] = (gender_pd['gender_gap'] / gender_pd['avg_men']) * 100

plt.title(f'Гендерный разрыв (в среднем {gender_pd["percent_gap"][0]:.1f}%)')
plt.ylabel('Зарплата ($)')
plt.xlabel('')
plt.xticks([0,1], ['Мужчины', 'Женщины'])
plt.show()

palette = {
    'white_men': '#aec7e8',
    'white_women': '#1f77b4',
    'black_men': '#ffbb78',
    'black_women': '#ff7f0e',
    'hispanic_men': '#98df8a',
    'hispanic_women': '#2ca02c'
}

# Комбинированный анализ (раса + пол)
spark.sql("SELECT \
    ROUND(AVG(white_men_bachelors_degree),2) AS white_men, \
    ROUND(AVG(white_women_bachelors_degree),2) AS white_women, \
    ROUND(AVG(black_men_bachelors_degree),2) AS black_men, \
    ROUND(AVG(black_women_bachelors_degree),2) AS black_women, \
    ROUND(AVG(hispanic_men_bachelors_degree),2) AS hispanic_men, \
    ROUND(AVG(hispanic_women_bachelors_degree),2) AS hispanic_women \
FROM hive_wages").show()

plt.figure(figsize=(12, 6))

for col in ['white_men_bachelors_degree', 'white_women_bachelors_degree',
            'black_men_bachelors_degree', 'black_women_bachelors_degree',
            'hispanic_men_bachelors_degree', 'hispanic_women_bachelors_degree']:
    group_name = col.replace('_bachelors_degree', '').replace('_', ' ')
    sns.lineplot(data=df_pd, x='year', y=col,
                color=palette[col.replace('_bachelors_degree', '')],
                linewidth=2,
                label=group_name.title())

plt.title('Зарплаты бакалавров по расе и полу', fontsize=14)
plt.ylabel('Зарплата ($)', fontsize=12)
plt.xlabel('Год', fontsize=12)
plt.legend(title='Группа', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, linestyle='--', alpha=0.3)
plt.tight_layout()
plt.show()