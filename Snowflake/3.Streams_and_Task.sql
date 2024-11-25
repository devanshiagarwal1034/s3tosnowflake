CREATE OR REPLACE STREAM customer_bookings_stream ON TABLE CUSTOMER_BOOKINGS_STAGING;


CREATE OR REPLACE TASK customer_bookings_task
WAREHOUSE = COMPUTE_WH 
SCHEDULE = '5 MINUTE' -- Adjust frequency as needed
AS
MERGE INTO TRAVEL_DB.RAW.CUSTOMER_BOOKINGS_DATA target
USING (
    SELECT *
    FROM TRAVEL_DB.RAW.CUSTOMER_BOOKINGS_STAGING
    WHERE metadata$action = 'INSERT'
) source
ON target.BOOKING_ID = source.BOOKING_ID -- Match based on the unique booking identifier
WHEN MATCHED THEN
    UPDATE SET
        target.CURRENCY_CODE = source.CURRENCY_CODE,
        target.STATUS = source.STATUS,
        target.PHONE_NUMBER = source.PHONE_NUMBER,
        target.BOOKING_DATE = source.BOOKING_DATE,
        target.SEGMENT_ID = source.SEGMENT_ID,
        target.FIRST_NAME = source.FIRST_NAME,
        target.DESTINATION_TYPE = source.DESTINATION_TYPE,
        target.CUSTOMER_ID = source.CUSTOMER_ID,
        target.AMOUNT_SPENT = source.AMOUNT_SPENT,
        target.COUNTRY_CODE = source.COUNTRY_CODE,
        target.ADDRESS = source.ADDRESS,
        target.EMAIL = source.EMAIL,
        target.LAST_NAME = source.LAST_NAME,
        target.BOOKING_TIME = source.BOOKING_TIME
WHEN NOT MATCHED THEN
    INSERT (
        CURRENCY_CODE,
        STATUS,
        PHONE_NUMBER,
        BOOKING_DATE,
        SEGMENT_ID,
        FIRST_NAME,
        BOOKING_ID,
        DESTINATION_TYPE,
        CUSTOMER_ID,
        AMOUNT_SPENT,
        COUNTRY_CODE,
        ADDRESS,
        EMAIL,
        LAST_NAME,
        BOOKING_TIME
    )
    VALUES (
        source.CURRENCY_CODE,
        source.STATUS,
        source.PHONE_NUMBER,
        source.BOOKING_DATE,
        source.SEGMENT_ID,
        source.FIRST_NAME,
        source.BOOKING_ID,
        source.DESTINATION_TYPE,
        source.CUSTOMER_ID,
        source.AMOUNT_SPENT,
        source.COUNTRY_CODE,
        source.ADDRESS,
        source.EMAIL,
        source.LAST_NAME,
        source.BOOKING_TIME
    );
TRUNCATE TABLE CUSTOMER_BOOKINGS_STAGING;
END;



ALTER TASK customer_bookings_task RESUME;
    
SHOW TASKS LIKE 'customer_bookings_task';
