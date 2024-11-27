CREATE OR REPLACE PROCEDURE process_customer_bookings()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Step 1: MERGE operation
    MERGE INTO TRAVEL_DB.RAW.CUSTOMER_BOOKINGS_DATA AS target
    USING TRAVEL_DB.RAW.CUSTOMER_BOOKINGS_STAGING AS source
    ON target.CUSTOMER_ID = source.CUSTOMER_ID
    WHEN MATCHED THEN
        UPDATE SET
            target.CURRENCY_CODE = source.CURRENCY_CODE,
            target.STATUS = source.STATUS,
            target.PHONE_NUMBER = source.PHONE_NUMBER,
            target.BOOKING_DATE = source.BOOKING_DATE,
            target.SEGMENT_ID = source.SEGMENT_ID,
            target.FIRST_NAME = source.FIRST_NAME,
            target.DESTINATION_TYPE = source.DESTINATION_TYPE,
            target.BOOKING_ID = source.BOOKING_ID,
            target.AMOUNT_SPENT = source.AMOUNT_SPENT,
            target.COUNTRY_CODE = source.COUNTRY_CODE,
            target.ADDRESS = source.ADDRESS,
            target.EMAIL = source.EMAIL,
            target.LAST_NAME = source.LAST_NAME,
            target.BOOKING_TIME = source.BOOKING_TIME
    WHEN NOT MATCHED THEN
        INSERT (CURRENCY_CODE, STATUS, PHONE_NUMBER, BOOKING_DATE, SEGMENT_ID, FIRST_NAME, BOOKING_ID, DESTINATION_TYPE, CUSTOMER_ID, AMOUNT_SPENT, COUNTRY_CODE, ADDRESS, EMAIL, LAST_NAME, BOOKING_TIME)
        VALUES (source.CURRENCY_CODE, source.STATUS, source.PHONE_NUMBER, source.BOOKING_DATE, source.SEGMENT_ID, source.FIRST_NAME, source.BOOKING_ID, source.DESTINATION_TYPE, source.CUSTOMER_ID, source.AMOUNT_SPENT, source.COUNTRY_CODE, source.ADDRESS, source.EMAIL, source.LAST_NAME, source.BOOKING_TIME);

    -- Step 2: TRUNCATE staging table
    TRUNCATE TABLE TRAVEL_DB.RAW.CUSTOMER_BOOKINGS_STAGING;

    RETURN 'Procedure executed successfully';
END;
$$;



CREATE OR REPLACE TASK customer_bookings_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE' -- Adjust the schedule as needed
AS
CALL process_customer_bookings();


ALTER TASK customer_bookings_task RESUME;



