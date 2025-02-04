#r "nuget: Dapper.FSharp"
#r "nuget: DuckDB.NET.Data"
#r "nuget: DuckDB.NET.Data.Full"

open DuckDB.NET.Data
open System.IO

let writeLocationsCsvByRegion (connection: DuckDBConnection) (baseInputPath: string) (regions: string list) (suffix: string) =
    let regionsStr = regions |> List.map (sprintf "'%s'") |> String.concat ","
    let command = connection.CreateCommand()
    let outputPath = baseInputPath.Replace(".csv", $"_{suffix}.csv")
    
    command.CommandText <- $"""
        COPY (
            SELECT * 
            FROM read_csv_auto('{baseInputPath}')
            WHERE region IN ({regionsStr})
        ) TO '{outputPath}' (FORMAT CSV, HEADER);
    """
    
    printfn "Writing locations CSV file to %s" outputPath
    command.ExecuteNonQuery() |> ignore

let processAllLocationRegions (connection: DuckDBConnection) (inputFilePath: string) =
    // Individual region passes
    let singleRegionPasses = [
        (["South"], "South")
        (["Northeast"], "Northeast")
        (["Midwest"], "Midwest")
        (["West"], "West")
    ]
    
    // Process individual regions
    for (regions, suffix) in singleRegionPasses do
        writeLocationsCsvByRegion connection inputFilePath regions suffix
        
    // Process all regions together
    let allRegions = ["South"; "Northeast"; "Midwest"; "West"]
    writeLocationsCsvByRegion connection inputFilePath allRegions "US"

// Usage
let connection = new DuckDBConnection("DataSource=:memory:")
connection.Open()

let inputPath = Path.Combine(__SOURCE_DIRECTORY__, "Files\dim_locations.csv")
processAllLocationRegions connection inputPath

connection.Close()