package com.weathermonitoring.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Nested "weather" object inside a WeatherStatusMessage.
 */
public class Weather {

    @JsonProperty("humidity")
    private int humidity; // percentage

    @JsonProperty("temperature")
    private int temperature; // fahrenheit

    @JsonProperty("wind_speed")
    private int windSpeed; // km/h

    public Weather() {
    }

    public Weather(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(int windSpeed) {
        this.windSpeed = windSpeed;
    }

    @Override
    public String toString() {
        return "Weather{humidity=" + humidity + ", temperature=" + temperature + ", windSpeed=" + windSpeed + '}';
    }
}
