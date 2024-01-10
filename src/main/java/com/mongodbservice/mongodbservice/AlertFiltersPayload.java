package com.mongodbservice.mongodbservice;

import org.json.JSONArray;
import org.json.JSONObject;

public class AlertFiltersPayload {
    private int driver_assigned;
    private JSONArray alert_video_status;
    private JSONArray fault_code;
    private JSONArray alert_severity;
    private JSONArray alert_type;
    private JSONArray weather_prediction_id;
    private JSONArray event_code;
    public static final String driverAssigned = "driver_assigned";
    public static final String videoStatus = "alert_video_status";
    public static final String fault = "fault_code";
    public static final String severity = "alert_severity";
    public static final String alertType = "alert_type";
    public static final String weather = "weather_prediction_id";
    public static final String eventCode = "event_code";

    AlertFiltersPayload(JSONObject payload){
        if(payload.has(driverAssigned))
            this.driver_assigned = payload.getInt(driverAssigned);
        if(payload.has(videoStatus))
            this.alert_video_status = payload.getJSONArray(videoStatus);
        if(payload.has(fault))
            this.fault_code = payload.getJSONArray(fault);
        if(payload.has(severity))
            this.alert_severity = payload.getJSONArray(severity);
        if(payload.has(alertType))
            this.alert_type = payload.getJSONArray(alertType);
        if(payload.has(eventCode))
            this.event_code = payload.getJSONArray(eventCode);
        if(payload.has(weather))
            this.weather_prediction_id = payload.getJSONArray(weather);
    }

    public int getDriver_assigned() {
        return driver_assigned;
    }

    public void setDriver_assigned(int driver_assigned) {
        this.driver_assigned = driver_assigned;
    }

    public JSONArray getAlert_video_status() {return alert_video_status;}

    public JSONArray getFault_code() {
        return fault_code;
    }

    public JSONArray getAlert_severity() {
        return alert_severity;
    }

    public JSONArray getAlert_type() {
        return alert_type;
    }

    public JSONArray getEvent_code() {
        return event_code;
    }

    public JSONArray getWeather_prediction_id() {return weather_prediction_id;}

    public JSONObject getJSON(){
        JSONObject out = new JSONObject();
        out.put("driver_assigned", driver_assigned);
        out.put("alert_severity", alert_severity);
        out.put("alert_video_status", alert_video_status);
        out.put("fault_code", fault_code);
        out.put("weather_prediction_id", weather_prediction_id);
        out.put("alert_types", alert_type);
        out.put("event_code", event_code);
        return out;
    }
}
