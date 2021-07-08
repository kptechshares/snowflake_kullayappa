--create file format 
create or replace file format my_csv_format type = 'csv' field_delimiter = '\t';

--Create a sample stage
create or replace stage user_stage url='s3://kpsampledata/'
credentials=(aws_key_id='Replace ID here' aws_secret_key='Replace secret here')
file_format = my_csv_format;

--create a sample table
create or replace table product( 
v1 varchar, v2 varchar);

--create metadata table
create table metadata(tablename varchar(50),pipename varchar(100));

--insert some dummy data
insert into metadata values('product','product_load');

--verify the metadata
select * from metadata;

--Create SP to create snowpipe in a iterative way
CREATE OR REPLACE PROCEDURE ADD_OBSERVATION_VALUES()
RETURNS string
LANGUAGE JAVASCRIPT
AS
$$
    var num_rows_sql = "SELECT tablename,pipename FROM metadata";
    var stmt = snowflake.createStatement( {sqlText: num_rows_sql} );
    var rows_result = stmt.execute(); 
    var value_array = [];
    
    while(rows_result.next()) {
		var stmt = "create or replace pipe "+rows_result.getColumnValue(2)+" auto_ingest=true as copy into "+rows_result.getColumnValue(1)+" from @user_stage";
		snowflake.createStatement({sqlText: stmt}).execute();    
    }
    
    
    return 'OK';
$$;
