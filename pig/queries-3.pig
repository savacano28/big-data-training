geoloc = LOAD '/user/root/geoloc/geolocation.csv' USING PigStorage(',') AS (truckid:chararray, driverid:chararray, event:chararray, latitude:double, longitude:double, city:chararray, state:chararray, velocity:double, event_ind:long, idling_ind:long);

truck_ids = GROUP geoloc BY truckid;

vel_mean = foreach truck_ids Generate group AS truckid, SUM(geoloc.velocity)/COUNT(geoloc) as mean;

STORE vel_mean INTO '/tmp/results-mean';

DUMP vel_mean;
