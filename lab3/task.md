# Lab3
Аникин Арсений, 6404-010302D

# RideCleanisingExercise

Задача упражнения  — очистить поток событий TaxiRide, удалив события, которые начинаются или заканчиваются за пределами Нью-Йорка.

Класс утилиты GeoUtils предоставляет статический метод isInNYC(float lon, float lat) для проверки, находится ли местоположение в пределах области Нью-Йорка.
Фильтр со статистическим методом isInNYC возращает true - если началньная и конечная точка поездки располагается в Нью-Йорке.

```java
public boolean filter(TaxiRide taxiRide) throws Exception {
	return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
}
```
Тесты:

![image](https://github.com/user-attachments/assets/31dc50c7-1959-485e-bcba-a2485add11ae)


# RidesAndFaresExercise

Цель упражнения — объединить записи TaxiRide и TaxiFare для каждой поездки. Метод flatMap1 обрабатывает элементы типа TaxiRide, проверяем fareState, если существует значение — сбрасываем fareState, и создаём кортеж. Для flatMap2 производим аналогичные действия для TaxiFare.

```java
private ValueState<TaxiRide> rideState;
private ValueState<TaxiFare> fareState;

@Override
public void open(Configuration config) throws Exception {
	rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
	fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
}

@Override
public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
	TaxiFare fare = fareState.value();
	if (fare != null) {
		fareState.clear();
		out.collect(new Tuple2<>(ride, fare));
	} else {
		rideState.update(ride);
	}
}

@Override
public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
	TaxiRide ride = rideState.value();
	if (ride != null) {
		rideState.clear();
		out.collect(new Tuple2<>(ride, fare));
	} else {
		fareState.update(fare);
	}
}
```

Тесты:

![image](https://github.com/user-attachments/assets/013dfe34-2718-47ed-b36d-cff7c8e5f33a)

# HourlyTipsExercise

Задача — определить водителя, который получает больше всего чаевых в час. Для этого по всем чаевым проводится группировка по водителю, за 1 час и применяется функция CalcTips - общая сумма чаевых за 1 час.

```java
DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
				.keyBy((TaxiFare fare) -> fare.driverId) // группируем поездки по водителю
				.timeWindow(Time.hours(1)) // окно времени - 1 час
				.process(new CalcTips()) // применяем функцию CalcTips
				.timeWindowAll(Time.hours(1)) // применяем функцию для 1 часа
				.maxBy(2); // максимизация по третьему элементу в кортеже - по сумме чаевых
```

```java
public static class CalcTips extends ProcessWindowFunction<
		TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
	@Override
	public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
		Float sumOfTips = Float.valueOf(0);
		for (TaxiFare fare : fares) {
			sumOfTips += fare.tip;
		}
		out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
	}
}
```

Тесты:

![image](https://github.com/user-attachments/assets/6aec8d46-a3f9-4d71-a46a-d4b031b38aea)

# LongRidesExercise

Цель — вывод предупреждения, когда поездка на такси длится более двух часов. Группируем по поездке и применяем функцию MatchFunction, используем состояние и таймер для отслеживания времени поездки.

```java
DataStream<TaxiRide> longRides = rides
				.keyBy(ride -> ride.rideId) // группировка по поездке
				.process(new MatchFunction());
```

```java
public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

	private ValueState<TaxiRide> rideState;

	@Override
	public void open(Configuration config) throws Exception {
		ValueStateDescriptor<TaxiRide> startDescriptor =
				new ValueStateDescriptor<>("saved ride", TaxiRide.class);
		rideState = getRuntimeContext().getState(startDescriptor); // создание состояния для хранения данных о поездке
	}

	@Override
	public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
		TimerService timerService = context.timerService();

		if (ride.isStart) { // Если начало поездки
			if (rideState.value() == null) {
				rideState.update(ride);
			}
		} else { // завершение
			rideState.update(ride);
		}
		// Через 2 часа после начала поездки проверить, была ли она завершена
		timerService.registerEventTimeTimer(ride.getEventTime() + 120 * 60 * 1000);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
		TaxiRide savedRide = rideState.value(); // Извлекаем сохраненную поездку
		if (savedRide != null && savedRide.isStart) {
			out.collect(savedRide); // отправляем информацию, если не завершена
		}
		rideState.clear();
	}
}
```

Тесты:

![image](https://github.com/user-attachments/assets/069de069-e555-42e5-8f78-059f59aa1052)
