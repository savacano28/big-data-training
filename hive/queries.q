--select truckid,count(city) from geolocation group by truckid;
select truckid,avg(velocity) from geolocation group by truckid;

select ip,count(ip) from sample_access_logs group by ip;
select ip, url_site from sample_access_logs where url_site rlike '.*/checkout';
select sb.prod,count(sb.prod) from (select regexp_extract(url_site,'product/(.*?)(/add_to_car)',1) prod from sample_access_logs where url_site rlike '.*/add_to_car' and code1='200') sb group by sb.prod order by prod asc;
select regexp_replace(sb.prod,'%20',' ') prod,count(sb.prod) from (select regexp_extract(url_site,'product/(.*?)(/add_to_car)',1) prod from sample_access_logs where url_site rlike '.*/add_to_car' and code1='200') sb group by sb.prod order by prod asc;
