open System.Collections.Generic
open DuckDB.NET.Data

let fileInputPath = "Files/"
let fileOutputPath = "Files/parquet/"

let loadCsvIntoDuckDB (connection: DuckDBConnection) (tableName: string) (filePath: string) =
    let command = connection.CreateCommand()
    command.CommandText <- $"""
        DROP TABLE IF EXISTS {tableName};
        CREATE TABLE {tableName} AS 
        SELECT * FROM read_csv('{filePath}', ignore_errors=true);
    """
    printfn "Dropping and loading table %s into DuckDB" tableName
    command.ExecuteNonQuery() |> ignore

let addNewColumns (connection: DuckDBConnection) (year: string) =
    let command = connection.CreateCommand()
    command.CommandText <- $"""
        ALTER TABLE CMS ADD COLUMN TOT_COSTS_PER_DAY decimal;
        ALTER TABLE CMS ADD COLUMN TOT_COSTS_PER_CLAIM decimal;
        ALTER TABLE CMS ADD COLUMN GE65_TOT_COSTS_PER_DAY decimal;
        ALTER TABLE CMS ADD COLUMN GE65_TOT_COSTS_PER_CLAIM decimal;
        ALTER TABLE CMS ADD COLUMN PRESCRIBER_ID int64;
        ALTER TABLE CMS ADD COLUMN LOCATION_ID int64;
        ALTER TABLE CMS ADD COLUMN MEDICATION_ID int64;
        ALTER TABLE CMS ADD COLUMN YEAR DATE DEFAULT DATE '{year}-01-01';
    """
    printfn "Adding new columns to CMS table"
    command.ExecuteNonQuery() |> ignore

let updateLocationIdColumn (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET LOCATION_ID = LOCATIONS.ID
            FROM LOCATIONS
            WHERE UPPER(CMS.Prscrbr_City) = UPPER(LOCATIONS.CITY)
            AND UPPER(CMS.Prscrbr_State_Abrvtn) = UPPER(LOCATIONS.STATE_CODE);
    """
    printfn "Updating LOCATION_ID column in CMS table"
    command.ExecuteNonQuery() |> ignore

let removeRowsWhereLocationIdIs9s (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- "DELETE FROM CMS WHERE LOCATION_ID = '99999';"
    printfn "Removing rows where LOCATION_ID is 99999"
    command.ExecuteNonQuery() |> ignore

let calculateTotalCostsPerDay (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET TOT_COSTS_PER_DAY = CMS.Tot_Drug_Cst / CMS.Tot_Day_Suply;
    """
    printfn "Calculating TOT_COSTS_PER_DAY column in CMS table"
    command.ExecuteNonQuery() |> ignore
    
let calculateTotalCostsPerClaim (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET TOT_COSTS_PER_CLAIM = CMS.Tot_Drug_Cst / CMS.Tot_Clms;
    """
    printfn "Calculating TOT_COSTS_PER_CLAIM column in CMS table"
    command.ExecuteNonQuery() |> ignore

let calculateGe65TotalCostsPerDay (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET GE65_TOT_COSTS_PER_DAY = CMS.GE65_Tot_Drug_Cst / CMS.GE65_Tot_Day_Suply;
    """
    printfn "Calculating GE65_TOT_COSTS_PER_DAY column in CMS table"
    command.ExecuteNonQuery() |> ignore
    
let calculateGe65TotalCostsPerClaim (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET GE65_TOT_COSTS_PER_CLAIM = CMS.GE65_Tot_Drug_Cst / CMS.GE65_Tot_Clms;
    """
    printfn "Calculating GE65_TOT_COSTS_PER_CLAIM column in CMS table"
    command.ExecuteNonQuery() |> ignore

let updateMedicationId (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET MEDICATION_ID = MEDICATIONS.ID
            FROM MEDICATIONS
            WHERE UPPER(CMS.Brnd_Name) = UPPER(MEDICATIONS.Brnd_Name);
    """
    printfn "Updating MEDICATION_ID column in CMS table"
    command.ExecuteNonQuery() |> ignore
    
let updatePrescriberIdColumn (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET PRESCRIBER_ID = PRESCRIBERS.ID
            FROM PRESCRIBERS
            WHERE CMS.Prscrbr_NPI = PRESCRIBERS.Prscrbr_NPI;
    """
    printfn "Updating PRESCRIBER_ID column in CMS table"
    command.ExecuteNonQuery() |> ignore

let removeUnneededColumns (connection: DuckDBConnection) =
    let columnsToDrop = [
        "Prscrbr_NPI"
        "Prscrbr_Last_Org_Name"
        "Prscrbr_First_Name"
        "Prscrbr_City"
        "Prscrbr_State_Abrvtn"
        "Prscrbr_State_FIPS"
        "Brnd_Name"
        "Gnrc_Name"
        "GE65_Bene_Sprsn_Flag"
        "GE65_Sprsn_Flag"
    ]
    for column in columnsToDrop do
        let command = connection.CreateCommand()
        command.CommandText <- $"ALTER TABLE CMS DROP COLUMN {column};"
        printfn "Removing column %s from CMS table" column
        command.ExecuteNonQuery() |> ignore

let writeParquetFile (connection: DuckDBConnection) (outputFilePath: string) =
    let command = connection.CreateCommand()
    command.CommandText <- $"COPY (SELECT * FROM CMS) TO '{outputFilePath}' (FORMAT 'parquet');"
    printfn "Writing parquet file to %s" outputFilePath
    command.ExecuteNonQuery() |> ignore

let fileDict =
    dict [
        2013, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.csv"
        2014, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.csv"
        2015, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.csv"
        2016, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.csv"
        2017, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.csv"
        2018, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.csv"
        2019, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.csv"
        2020, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.csv"
        2021, "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.csv"
        2022, "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"
    ]

let loadSupportTables (connection: DuckDBConnection) =
    loadCsvIntoDuckDB connection "LOCATIONS" (fileInputPath + "locations.csv")
    loadCsvIntoDuckDB connection "MEDICATIONS" (fileInputPath + "dim_medications.csv")
    loadCsvIntoDuckDB connection "PRESCRIBERS" (fileInputPath + "prescribers.csv")


let runTasksInSeries (fileDict: IDictionary<int, string>) connection =
    loadSupportTables connection
    for kvp in fileDict do
        let year = kvp.Key
        let fileName = kvp.Value
        let inputFilePath = fileInputPath + fileName
        let outputFilePath = fileOutputPath + $"CMS_MedPtD_PbyPaD_{year}.parquet"
        loadCsvIntoDuckDB connection "CMS" inputFilePath
        addNewColumns connection (year.ToString())
        calculateTotalCostsPerClaim connection
        calculateTotalCostsPerDay connection
        calculateGe65TotalCostsPerClaim connection
        calculateGe65TotalCostsPerDay connection
        updatePrescriberIdColumn connection
        updateLocationIdColumn connection
        updateMedicationId connection
        removeRowsWhereLocationIdIs9s connection
        removeUnneededColumns connection
        writeParquetFile connection outputFilePath

[<EntryPoint>]
let main argv =
    let connectionString = "Data Source=:memory:"
    use connection = new DuckDBConnection(connectionString)
    connection.Open()
    runTasksInSeries fileDict connection
    0