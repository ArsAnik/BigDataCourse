# Lab3
Аникин Арсений, 6404-010302D

# RideCleanisingExercise

Класс утилиты GeoUtils предоставляет статический метод isInNYC(float lon, float lat) для проверки, находится ли местоположение в пределах области Нью-Йорка.
Фильтр со статистическим методом isInNYC возращает true - если началньная и конечная точка поездки располагается в Нью-Йорке.

```
public boolean filter(TaxiRide taxiRide) throws Exception {
			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
```
