from prophet import Prophet

# 1. Подготовка данных
df_prophet = df[['year', 'gap_white_black_bachelors']].rename(columns={'year': 'ds', 'gap_white_black_bachelors': 'y'})
df_prophet['ds'] = pd.to_datetime(df_prophet['ds'], format='%Y')

# 2. Обучение модели
model = Prophet(yearly_seasonality=False)
model.fit(df_prophet)

# 3. Создание будущих дат до 2034 года
future = model.make_future_dataframe(periods=2035-2022, freq='Y')
forecast = model.predict(future)

# 4. Визуализация прогноза
fig = model.plot(forecast)
plt.title('Разрыв зарплат: Факт vs Прогноз до 2035 года (Prophet)')
plt.xlabel('Год')
plt.ylabel('Разрыв в заработной плате (долл./час)')
plt.grid(True)
plt.show()

# 5. Вывод прогноза только по годам 2023-2034
forecast_filtered = forecast[forecast['ds'].dt.year >= 2023][['ds', 'yhat']]
forecast_filtered['year'] = forecast_filtered['ds'].dt.year
forecast_filtered = forecast_filtered[['year', 'yhat']]
print(forecast_filtered.round(2).to_markdown())

from sklearn.model_selection import train_test_split

# Разделяем данные (80% на обучение, 20% на тест)
train, test = train_test_split(df_prophet, test_size=0.2, shuffle=False)

from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

# Прогноз для тестовой выборки
future_test = model.make_future_dataframe(periods=len(test), freq='Y')
forecast_test = model.predict(future_test)

# Фильтруем только тестовый период
test_pred = forecast_test[forecast_test['ds'].isin(test['ds'])]

# Рассчитываем метрики
mae = mean_absolute_error(test['y'], test_pred['yhat'])
mape = np.mean(np.abs((test['y'] - test_pred['yhat']) / test['y'])) * 100

print(f"""
Метрики качества на тестовой выборке:
- MAE (Средняя абсолютная ошибка): {mae:.2f}
- MAPE (Средняя абсолютная процентная ошибка): {mape:.2f}%
""")