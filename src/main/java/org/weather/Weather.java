package org.weather;

public class Weather {

    private final String date;
    public final String day;
    public final Double temperature;

    public Weather(String date, Double temperature) {
        this.date = date;
        this.day = this.date.split("\\s")[0];
        this.temperature = temperature;
    }





}
