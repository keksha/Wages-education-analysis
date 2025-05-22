-- Создание таблицы в MariaDB
CREATE TABLE wages_analysis (
    id INT,
    year INT,
    less_than_hs FLOAT,
    high_school FLOAT,
    some_college FLOAT,
    bachelors_degree FLOAT,
    advanced_degree FLOAT,
    white_bachelors_degree FLOAT,
    black_bachelors_degree FLOAT,
    hispanic_bachelors_degree FLOAT,
    men_bachelors_degree FLOAT,
    women_bachelors_degree FLOAT,
    white_men_bachelors_degree FLOAT,
    white_women_bachelors_degree FLOAT,
    black_men_bachelors_degree FLOAT,
    black_women_bachelors_degree FLOAT,
    hispanic_men_bachelors_degree FLOAT,
    hispanic_women_bachelors_degree FLOAT
);

-- Проверка структуры таблицы
DESCRIBE wages_analysis;

-- Выборка топ-5 строк
SELECT * FROM wages_analysis LIMIT 5;
