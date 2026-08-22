package com.weathermonitoring.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Matches the "Weather Status Message" schema from the lab spec:
 * {
 *   "station_id": 1,
 *   "s_no": 1,
 *   "battery_status": "low",
 *   "status_timestamp": 1681521224,
 *   "weather": { "humidity": 35, "temperature": 100, "wind_speed": 13 }
 * }
 */
public class WeatherStatusMessage {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sNo;

    @JsonProperty("battery_status")
    private String batteryStatus; // low | medium | high

    @JsonProperty("status_timestamp")
    private long statusTimestamp; // unix epoch seconds

    @JsonProperty("weather")
    private Weather weather;

    public WeatherStatusMessage() {
    }

    public WeatherStatusMessage(long stationId, long sNo, String batteryStatus, long statusTimestamp, Weather weather) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public long getStationId() {
        return stationId;
    }

    public void setStationId(long stationId) {
        this.stationId = stationId;
    }

    public long getsNo() {
        return sNo;
    }

    public void setsNo(long sNo) {
        this.sNo = sNo;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    public Weather getWeather() {
        return weather;
    }

    public void setWeather(Weather weather) {
        this.weather = weather;
    }

    @Override
    public String toString() {
        return "WeatherStatusMessage{stationId=" + stationId + ", sNo=" + sNo +
                ", batteryStatus='" + batteryStatus + '\'' + ", statusTimestamp=" + statusTimestamp +
                ", weather=" + weather + '}';
    }
}
