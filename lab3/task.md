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


# 
