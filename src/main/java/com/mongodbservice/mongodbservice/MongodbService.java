package com.mongodbservice.mongodbservice;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.json.JSONObject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Service
public class MongodbService {


    private final MongoTemplate mongoTemplate;

    public MongodbService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }
    public String getResults() {

        Instant startDate = Instant.parse("2023-09-01T00:00:00.00Z");
        Instant endDate = Instant.parse("2023-09-30T00:00:00.00Z");

        Criteria criteria = Criteria.where("tenant_id").is(11219);
        //.and("time_stamp").gte(startDate).lte(endDate)
        //.and("alert_severity").is("1");


        MatchOperation matchStage = Aggregation.match(criteria);

        GroupOperation groupStage1 = Aggregation.group()
                .count().as("count");
        GroupOperation groupStage2 = Aggregation.group("alert_type")
                .count().as("count");
        GroupOperation groupStage3 = Aggregation.group("alert_category")
                .count().as("count");
        GroupOperation groupStage4 = Aggregation.group("event_code")
                .count().as("count");
        GroupOperation groupStage5 = Aggregation.group("weather_prediction_id")
                .count().as("count");
        GroupOperation groupStage6 = Aggregation.group("alert_severity")
                .count().as("count");
        GroupOperation groupStage7 = Aggregation.group("alert_video_status")
                .count().as("count");
        //for top drivers
        GroupOperation groupStage8 = Aggregation.group("alert_type", "driver_id")
                .count().as("count");

        SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "count");
        AggregationOperation customOperation1 = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                return Document.parse("{"
                        + "$group: {"
                        + "_id: { alert_type: '$_id.alert_type' },"
                        + "Drivers: {"
                        + "$topN: {"
                        + "output: [{driver_id: '$_id.driver_id'}, {count: '$count'}],"
                        + "sortBy: { alert_type: 1 },"
                        + "n: 3"
                        + "}"
                        + "}"
                        + "}"
                        + "}");
            }
        };

        //for 3 docs for each alert type
        AggregationOperation customOperation2 = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                return Document.parse("{"
                        + "$group: {"
                        + "_id: '$alert_type',"
                        + "Docs: {"
                        + "$topN: {"
                        + "output: ['$$ROOT'],"
                        + "sortBy: { time_stamp: 1 },"
                        + "n: 3"
                        + "}"
                        + "}"
                        + "}"
                        + "}");
            }
        };


        FacetOperation facetOperation = Aggregation.facet()
                .and(groupStage1)
                .as("total_count")
                .and(groupStage2)
                .as("alert_type_count")
                .and(groupStage3)
                .as("alert_category_count")
                .and(groupStage4)
                .as("event_code_count")
                .and(groupStage5)
                .as("weather_prediction_id_count")
                .and(groupStage6)
                .as("alert_severity_count")
                .and(groupStage7)
                .as("alert_video_status_count")
                .and(groupStage8, sortStage, customOperation1)
                .as("topDrivers")
                .and(customOperation2)
                .as("Docs");


        Aggregation aggregation = Aggregation.newAggregation(matchStage, facetOperation);

        AggregationResults<JSONObject> results = mongoTemplate.aggregate(aggregation, "sample", JSONObject.class);
        //System.out.println(results.getRawResults().toJson());

        return results.getRawResults().toJson();
    }
    public String getSmartView(String requestPayload, int tenantId, String startDate, String endDate) {

        long unix_start_date = Long.parseLong(startDate);
        long unix_end_date = Long.parseLong(endDate);
        long startTimeMillis = unix_start_date * 1000; // if startTime is in seconds
        long endTimeMillis = unix_end_date * 1000; // if endTime is in seconds

        AlertFiltersPayload payload = new AlertFiltersPayload(new JSONObject(requestPayload).getJSONObject("filters"));


        List<Criteria> criteriaList = new ArrayList<>();


        // Define criteria for tenant_id and time_stamp
        Criteria tenantCriteria = Criteria.where("tenant_id").is(11219);
        Criteria timeCriteria = Criteria.where("time_stamp").gte(new Date(startTimeMillis)).lte(new Date(endTimeMillis));
        criteriaList.add(tenantCriteria);
        criteriaList.add(timeCriteria);

        // Define criteria for alert_type
        if (payload.getAlert_type() != null && payload.getAlert_type().toList().stream().noneMatch(type -> type.equals("ALL"))) {
            List<Criteria> alertTypeCriterias = new ArrayList<>();
            payload.getAlert_type().forEach(alertType -> alertTypeCriterias.add(Criteria.where("alert_type").is(alertType)));
            Criteria alertTypeCriteria = new Criteria().orOperator(alertTypeCriterias.toArray(new Criteria[alertTypeCriterias.size()]));
            criteriaList.add(alertTypeCriteria);
        }
        // Define criteria for alert_severity
        if (payload.getAlert_severity() != null) {
            List<Criteria> alertSeverityCriterias = new ArrayList<>();
            payload.getAlert_severity().forEach(alertSeverity -> alertSeverityCriterias.add(Criteria.where("alert_severity").is(alertSeverity)));
            Criteria alertSeverityCriteria = new Criteria().orOperator(alertSeverityCriterias.toArray(new Criteria[alertSeverityCriterias.size()]));
            criteriaList.add(alertSeverityCriteria);
        }

        // Define criteria for alert_video_status
        if (payload.getAlert_video_status() != null) {
            List<Criteria> alertVideoStatusCriterias = new ArrayList<>();
            payload.getAlert_video_status().forEach(alertVideoStatus -> alertVideoStatusCriterias.add(Criteria.where("alert_video_status").is(alertVideoStatus)));
            Criteria alertVideoStatusCriteria = new Criteria().orOperator(alertVideoStatusCriterias.toArray(new Criteria[alertVideoStatusCriterias.size()]));
            criteriaList.add(alertVideoStatusCriteria);
        }

        // Define criteria for event_code
        if (payload.getEvent_code() != null) {
            List<Criteria> eventCodeCriterias = new ArrayList<>();
            payload.getEvent_code().forEach(eventCode -> eventCodeCriterias.add(Criteria.where("event_code").is(eventCode)));
            Criteria eventCodeCriteria = new Criteria().orOperator(eventCodeCriterias.toArray(new Criteria[eventCodeCriterias.size()]));
            criteriaList.add(eventCodeCriteria);
        }

        // Define criteria for fault_code
        if (payload.getFault_code() != null) {
            List<Criteria> faultCodeCriterias = new ArrayList<>();
            payload.getFault_code().forEach(faultCode -> faultCodeCriterias.add(Criteria.where("fault_code").is(faultCode)));
            Criteria faultCodeCriteria = new Criteria().orOperator(faultCodeCriterias.toArray(new Criteria[faultCodeCriterias.size()]));
            criteriaList.add(faultCodeCriteria);
        }

        // Define criteria for weather_prediction_id
        if (payload.getWeather_prediction_id() != null) {
            List<Criteria> weatherPredictionIdCriterias = new ArrayList<>();
            payload.getWeather_prediction_id().forEach(weatherPredictionId -> weatherPredictionIdCriterias.add(Criteria.where("weather_prediction_id").is(weatherPredictionId)));
            Criteria weatherPredictionIdCriteria = new Criteria().orOperator(weatherPredictionIdCriterias.toArray(new Criteria[weatherPredictionIdCriterias.size()]));
            criteriaList.add(weatherPredictionIdCriteria);
        }

        // Define criteria for driver_assigned
//        Criteria driverAssignedCriteria = Criteria.where("driver_assigned").is(payload.getDriver_assigned());
//        criteriaList.add(driverAssignedCriteria);

        //Combine all criteria with AND operator
        Criteria criteria = new Criteria().andOperator(criteriaList.toArray(new Criteria[criteriaList.size()]));
        System.out.println(criteria.getCriteriaObject().toJson());
        MatchOperation matchStage = Aggregation.match(criteria);


        GroupOperation groupStage1 = Aggregation.group()
                .count().as("count");
        GroupOperation groupStage2 = Aggregation.group("alert_type")
                .count().as("count");
        GroupOperation groupStage3 = Aggregation.group("alert_category")
                .count().as("count");
        GroupOperation groupStage4 = Aggregation.group("event_code")
                .count().as("count");
        GroupOperation groupStage5 = Aggregation.group("weather_prediction_id")
                .count().as("count");
        GroupOperation groupStage6 = Aggregation.group("alert_severity")
                .count().as("count");
        GroupOperation groupStage7 = Aggregation.group("alert_video_status")
                .count().as("count");
        GroupOperation groupStage8 = Aggregation.group("fault_code")
                .count().as("count");


        //for top drivers
        GroupOperation groupStage9 = Aggregation.group("alert_type", "driver_id")
                .count().as("count");

        SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "count");
        AggregationOperation customOperation1 = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                return Document.parse("{"
                        + "$group: {"
                        + "_id: { alert_type: '$_id.alert_type' },"
                        + "Drivers: {"
                        + "$topN: {"
                        + "output: [{driver_id: '$_id.driver_id'}, {count: '$count'}],"
                        + "sortBy: { alert_type: 1 },"
                        + "n: 3"
                        + "}"
                        + "}"
                        + "}"
                        + "}");
            }
        };

        //for 3 docs for each alert type
        AggregationOperation customOperation2 = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                return Document.parse("{"
                        + "$group: {"
                        + "_id: '$alert_type',"
                        + "Docs: {"
                        + "$topN: {"
                        + "output: ['$$ROOT'],"
                        + "sortBy: { time_stamp: 1 },"
                        + "n: 3"
                        + "}"
                        + "}"
                        + "}"
                        + "}");
            }
        };
        FacetOperation facetOperation = Aggregation.facet()
                .and(groupStage1)
                .as("total_count")
                .and(groupStage2)
                .as("alert_type_count")
                .and(groupStage3)
                .as("alert_category_count")
                .and(groupStage4)
                .as("event_code_count")
                .and(groupStage5)
                .as("weather_prediction_id_count")
                .and(groupStage6)
                .as("alert_severity_count")
                .and(groupStage7)
                .as("alert_video_status_count")
                .and(groupStage8)
                .as("fault_code_count")
                .and(groupStage9, sortStage, customOperation1)
                .as("topDrivers")
                .and(customOperation2)
                .as("Docs");


        Aggregation aggregation = Aggregation.newAggregation(matchStage, facetOperation);

        AggregationResults<JSONObject> results = mongoTemplate.aggregate(aggregation, "sample", JSONObject.class);
        //System.out.println(results.getRawResults().toJson());

        return results.getRawResults().toJson();


    }
}
