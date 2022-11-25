package com.ksb.dto;

public class CityInfo {
    private Long cityId;
    private String cityName;
    private Long cityPop;

    public Long getCityId() { return cityId;  }

    public void setCityId(Long cityId) { this.cityId = cityId; }

    public String getCityName() { return cityName; }

    public void setName(String cityName) {
        this.cityName = cityName;
    }

    public Long getCityPop() {
        return cityPop;
    }

    public void setCityPop(Long cityPop) {
        this.cityPop = cityPop;
    }

}
