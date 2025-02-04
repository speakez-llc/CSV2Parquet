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

let fixPrescriberTypes (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        UPDATE CMS
            SET Prscrbr_Type = CASE
                WHEN Prscrbr_Type = 'Allergy/ Immunology' THEN 'Allergy & Immunology'
                WHEN Prscrbr_Type = 'Allergy/Immunology' THEN 'Allergy & Immunology'
                WHEN Prscrbr_Type = 'Behavior Analyst' THEN 'Behavioral Analyst'
                WHEN Prscrbr_Type = 'Clinical Cardiac Electrophysiology' THEN 'Clinical Cardiatric Electrophysiology'
                WHEN Prscrbr_Type = 'Colorectal Surgery (formerly proctology)' THEN 'Colorectal Surgery (Proctology)'
                WHEN Prscrbr_Type = 'Gynecological/Oncology' THEN 'Gynecological Oncology'
                WHEN Prscrbr_Type = 'Hematology/Oncology' THEN 'Hematology-Oncology'
                WHEN Prscrbr_Type = 'Obstetrics/Gynecology' THEN 'Obstetrics & Gynecology'
                WHEN Prscrbr_Type = 'Oral Surgery (dentists only)' THEN 'Oral Surgery (Dentist only)'
                WHEN Prscrbr_Type = 'Orthopaedic Surgery' THEN 'Orthopedic Surgery'
                WHEN Prscrbr_Type = 'Physical Medicine and Rehabilitation' THEN 'Physical Medicine & Rehabilitation'
                WHEN Prscrbr_Type = 'Plastic and Reconstructive Surgery' THEN 'Plastic & Reconstructive Surgery'
                WHEN Prscrbr_Type = 'Registered Dietician/Nutrition Professional' THEN 'Registered Dietician or Nutrition Professional'
                WHEN Prscrbr_Type = 'Specialist/Technologist, Health Information' THEN 'Specialist/Technologist'
            END;
    """
    printfn "Fixing prescriber types in CMS table"
    command.ExecuteNonQuery() |> ignore

let removeNonUSEntries (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        delete from cms
            where Prscrbr_State_Abrvtn not in (
                select distinct state_code 
                from dim_locations
            );
        """
    printfn "Fixing non-US entries in CMS table"
    command.ExecuteNonQuery() |> ignore


let addNewColumns (connection: DuckDBConnection) (year: string) =
    let command = connection.CreateCommand()
    command.CommandText <- $"""
        alter table cms add column prescriber_id int64;
        alter table cms add column location_id int64;
        alter table cms add column medication_id int64;
        alter table cms add column year text;
    """
    printfn "Adding new columns to CMS table"
    command.ExecuteNonQuery() |> ignore

let setYearColumn (connection: DuckDBConnection) (year: string) =
    let cmd = connection.CreateCommand()
    cmd.CommandText <- $"UPDATE cms SET year = '{year}';"
    cmd.ExecuteNonQuery() |> ignore

let updateLocationIdColumn (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        update cms
            set location_id = dim_locations.id
            from dim_locations
            where upper(cms.prscrbr_city) = upper(dim_locations.city)
            and upper(cms.prscrbr_state_abrvtn) = upper(dim_locations.state_code);
    """
    printfn "updating location_id column in cms table"
    command.ExecuteNonQuery() |> ignore

let removeRowsWhereLocationIdIsBlank (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- "delete from cms where Location_Id IS NULL;"
    printfn "removing rows where Location_Id is blank"
    command.ExecuteNonQuery() |> ignore

let updateMedicationId (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        update cms
            set medication_id = dim_medications.id
            from dim_medications
            where cms.brnd_name = dim_medications.brand_name;
    """
    printfn "updating medication_id column in cms table"
    command.ExecuteNonQuery() |> ignore
    
let updatePrescriberIdColumn (connection: DuckDBConnection) =
    let command = connection.CreateCommand()
    command.CommandText <- """
        update cms
            set prescriber_id = dim_prescribers.id
            from dim_prescribers
            where cms.Prscrbr_NPI = dim_prescribers.prescriber_npi;
    """
    printfn "updating prescriber_id column in cms table"
    command.ExecuteNonQuery() |> ignore

let removeUnneededColumns (connection: DuckDBConnection) =
    let columnsToDrop = [
        "Prscrbr_NPI"
        "Prscrbr_Type_Src"
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
        command.CommandText <- $"ALTER TABLE cms DROP COLUMN {column};"
        printfn "Removing column %s from cms table" column
        command.ExecuteNonQuery() |> ignore

let columnNames = [
    ("Prscrbr_Type", "prescriber_type")
    ("Tot_Clms", "total_claims_under_65")
    ("Tot_30Day_Fills", "total_30_day_fills_under_65")
    ("Tot_Day_Suply", "total_day_supply_under_65")
    ("Tot_Drug_Cst", "total_drug_cost_under_65")
    ("Tot_Benes", "total_beneficiaries_under_65")
    ("GE65_Tot_Clms", "total_claims_65_or_older")
    ("GE65_Tot_30Day_Fills", "total_30_day_fills_65_or_older")
    ("GE65_Tot_Day_Suply", "total_day_supply_65_or_older")
    ("GE65_Tot_Drug_Cst", "total_drug_cost_65_or_older")
    ("GE65_Tot_Benes", "total_beneficiaries_65_or_older")
]

let renameColumn (connection: DuckDBConnection) (columnNames: string * string) =
    let (currentName, newName) = columnNames
    let command = connection.CreateCommand()
    command.CommandText <- $"ALTER TABLE cms RENAME COLUMN {currentName} TO {newName};"
    printfn "Renaming column %s to %s in cms table" currentName newName
    command.ExecuteNonQuery() |> ignore 

let lowerCaseAllColumns (connection: DuckDBConnection) (tableName: string) =
    // Get all column names
    let command = connection.CreateCommand()
    command.CommandText <- $"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{tableName}'
    """
    
    use reader = command.ExecuteReader()
    let columns = [
        while reader.Read() do
            yield reader.GetString(0)
    ]
    
    // Rename each column to lowercase
    for column in columns do
        let lowerName = column.ToLower()
        if column <> lowerName then
            let renameCmd = connection.CreateCommand()
            renameCmd.CommandText <- $"ALTER TABLE {tableName} RENAME COLUMN {column} TO {lowerName};"
            printfn "Renaming column %s to %s in %s table" column lowerName tableName
            renameCmd.ExecuteNonQuery() |> ignore

let writeParquetFileByRegion (connection: DuckDBConnection) (baseOutputPath: string) (regions: string list) (suffix: string) =
    let regionsStr = regions |> List.map (sprintf "'%s'") |> String.concat ","
    let command = connection.CreateCommand()
    let outputPath = baseOutputPath.Replace(".parquet", $"_{suffix}.parquet")
    
    command.CommandText <- $"""
        COPY (
            SELECT cms.* 
            FROM cms 
            JOIN dim_locations ON cms.location_id = dim_locations.id 
            WHERE dim_locations.region IN ({regionsStr})
        ) TO '{outputPath}' (FORMAT 'parquet');
    """
    
    printfn "Writing parquet file to %s" outputPath
    command.ExecuteNonQuery() |> ignore

let processAllRegions (connection: DuckDBConnection) (outputFilePath: string) =
    // Individual region passes
    let singleRegionPasses = [
        (["South"], "South")
        (["Northeast"], "Northeast")
        (["Midwest"], "Midwest")
        (["West"], "West")
    ]
    
    // Process individual regions
    for (regions, suffix) in singleRegionPasses do
        writeParquetFileByRegion connection outputFilePath regions suffix
        
    // Process all regions together
    let allRegions = ["South"; "Northeast"; "Midwest"; "West"]
    writeParquetFileByRegion connection outputFilePath allRegions "US"

let fileDict =
    dict [
        "2013", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.csv"
        "2014", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2014.csv"
        "2015", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2015.csv"
        "2016", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2016.csv"
        "2017", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2017.csv"
        "2018", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2018.csv"
        "2019", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2019.csv"
        "2020", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2020.csv"
        "2021", "Medicare_Part_D_Prescribers_by_Provider_and_Drug_2021.csv"
        "2022", "MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"
    ]

let loadSupportTables (connection: DuckDBConnection) =
    loadCsvIntoDuckDB connection "dim_locations" (fileInputPath + "dim_locations.csv")
    loadCsvIntoDuckDB connection "dim_medications" (fileInputPath + "dim_medications.csv")
    loadCsvIntoDuckDB connection "dim_prescribers" (fileInputPath + "dim_prescribers.csv")



let runTasksInSeries (fileDict: IDictionary<string, string>) connection =
    loadSupportTables connection
    for kvp in fileDict do
        let year = kvp.Key
        let fileName = kvp.Value
        let inputFilePath = fileInputPath + fileName
        let outputFilePath = fileOutputPath + $"CMS_MedPtD_PbyPaD_{year}.parquet"
        loadCsvIntoDuckDB connection "CMS" inputFilePath
        fixPrescriberTypes connection
        removeNonUSEntries connection
        addNewColumns connection (year)
        setYearColumn connection (year)
        updatePrescriberIdColumn connection
        updateLocationIdColumn connection
        removeRowsWhereLocationIdIsBlank connection
        updateMedicationId connection
        removeUnneededColumns connection
        for column in columnNames do
            renameColumn connection column
        lowerCaseAllColumns connection "cms"
        processAllRegions connection outputFilePath

[<EntryPoint>]
let main argv =
    let connectionString = "Data Source=:memory:"
    use connection = new DuckDBConnection(connectionString)
    connection.Open()
    runTasksInSeries fileDict connection
    0