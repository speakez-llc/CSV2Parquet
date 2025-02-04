
CREATE TABLE dim_locations AS SELECT * FROM read_csv('dim_locations.csv');
CREATE TABLE dim_prescribers AS SELECT * FROM read_csv('dim_prescribers.csv');
CREATE TABLE dim_medications AS SELECT * FROM read_csv('dim_medications.csv');
CREATE TABLE cms AS SELECT * FROM read_parquet('D:/repos/RexData/*.parquet');
DELETE FROM dim_locations WHERE REGION NOT IN ('West');
DELETE FROM CMS WHERE location_id NOT IN (SELECT DISTINCT ID FROM dim_locations WHERE REGION in ('West', 'Midwest', 'South', 'Northeast'));

