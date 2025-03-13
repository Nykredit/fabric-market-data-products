-- Create the stored procedure.
CREATE PROCEDURE [dbo].[copy_data_from_lakehouse]
AS
BEGIN
    -- Drop the destination table if it already exists.
    DROP TABLE IF EXISTS [dbo].[Instrument_Bond_CouponRate_Copy];

    -- Create the destination table.
    CREATE TABLE [dbo].[Instrument_Bond_CouponRate_Copy] (
        [Instrument_GainId] [VARCHAR],
        [Date] [VARCHAR],
        [FixingRate] [FLOAT],
        [InterestRate] [FLOAT],
        [MetaData_GoldUniqueId] [VARCHAR]
    );

    -- Copy data from the Lakehouse to the Warehouse.
    INSERT INTO [dbo].[Instrument_Bond_CouponRate_Copy]
    SELECT * FROM [DMS_LH_Gold].[Instrument_Bond_CouponRate];
END;