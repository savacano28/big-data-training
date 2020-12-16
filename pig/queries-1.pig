geoloc = LOAD '/user/root/geoloc/geolocation.csv' USING PigStorage(',') AS (truckid:chararray, driverid:chararray, event:chararray, latitude:double, longitude:double, city:chararray, state:chararray, velocity:double, event_ind:long, idling_ind:long);

truck_ids = GROUP geoloc BY truckid;

result = foreach truck_ids {
unique = DISTINCT geoloc.city; 
generate group, COUNT(unique) as cities;
};

--STORE result INTO '/tmp/results-cities';

DUMP result;
