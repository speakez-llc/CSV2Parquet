open System
open System.IO
open ParquetSharp
open ParquetSharp.Schema

type MedicareDataRow = {
    Prscrbr_NPI: string
    Prscrbr_Last_Org_Name: string
    Prscrbr_First_Name: string
    Prscrbr_City: string
    Prscrbr_State_Abrvtn: string
    Prscrbr_State_FIPS: string
    Prscrbr_Type: string
    Prscrbr_Type_Src: string
    Brnd_Name: string
    Gnrc_Name: string
    Tot_Clms: Nullable<int64>
    Tot_30day_Fills: Nullable<decimal>
    Tot_Day_Suply: Nullable<int64>
    Tot_Drug_Cst: Nullable<decimal>
    Tot_Benes: Nullable<int64>
    GE65_Sprsn_Flag: string
    GE65_Tot_Clms: Nullable<int64>
    GE65_Tot_30day_Fills: Nullable<decimal>
    GE65_Tot_Drug_Cst: Nullable<decimal>
    GE65_Tot_Day_Suply: Nullable<int64>
    GE65_Tot_Benes: Nullable<int64>
    GE65_Bene_Sprsn_Flag: string
}

let parseNullableInt64 (s: string) =
    match Int64.TryParse(s) with
    | true, v -> Nullable(v)
    | _ -> Nullable()

let parseNullableDecimal (s: string) (scale: int) =
    match Decimal.TryParse(s) with
    | true, v ->
        let factor = decimal (Math.Pow(10.0, float scale))
        Nullable<decimal>(Math.Round(v * factor) / factor)
    | _ -> Nullable<decimal>()

let parseCsvRow (line: string) =
    let columns = line.Split(',')
    {
        Prscrbr_NPI = columns[0]
        Prscrbr_Last_Org_Name = columns[1]
        Prscrbr_First_Name = columns[2]
        Prscrbr_City = columns[3]
        Prscrbr_State_Abrvtn = columns[4]
        Prscrbr_State_FIPS = columns[5]
        Prscrbr_Type = columns[6]
        Prscrbr_Type_Src = columns[7]
        Brnd_Name = columns[8]
        Gnrc_Name = columns[9]
        Tot_Clms = parseNullableInt64 columns[10]
        Tot_30day_Fills = parseNullableDecimal columns[11] 2
        Tot_Day_Suply = parseNullableInt64 columns[12]
        Tot_Drug_Cst = parseNullableDecimal columns[13] 2
        Tot_Benes = parseNullableInt64 columns[14]
        GE65_Sprsn_Flag = columns[15]
        GE65_Tot_Clms = parseNullableInt64 columns[16]
        GE65_Tot_30day_Fills = parseNullableDecimal columns[17] 2
        GE65_Tot_Drug_Cst = parseNullableDecimal columns[18] 2
        GE65_Tot_Day_Suply = parseNullableInt64 columns[19]
        GE65_Tot_Benes = parseNullableInt64 columns[20]
        GE65_Bene_Sprsn_Flag = columns[21]
    }

let loadCsv (filePath: string) =
    File.ReadLines(filePath)
    |> Seq.skip 1 // Skip header
    |> Seq.map parseCsvRow

let roundNullableDecimal (value: Nullable<decimal>) (scale: int) =
    if value.HasValue then
        let factor = decimal (Math.Pow(10.0, float scale))
        Nullable<decimal>(Math.Round(value.Value * factor) / factor)
    else
        Nullable<decimal>()

let rowToBoxedArray (row: MedicareDataRow) =
    [|
        box row.Prscrbr_NPI
        box row.Prscrbr_Last_Org_Name
        box row.Prscrbr_First_Name
        box row.Prscrbr_City
        box row.Prscrbr_State_Abrvtn
        box row.Prscrbr_State_FIPS
        box row.Prscrbr_Type
        box row.Prscrbr_Type_Src
        box row.Brnd_Name
        box row.Gnrc_Name
        box row.Tot_Clms
        box (roundNullableDecimal row.Tot_30day_Fills 2)
        box row.Tot_Day_Suply
        box (roundNullableDecimal row.Tot_Drug_Cst 2)
        box row.Tot_Benes
        box row.GE65_Sprsn_Flag
        box row.GE65_Tot_Clms
        box (roundNullableDecimal row.GE65_Tot_30day_Fills 2)
        box (roundNullableDecimal row.GE65_Tot_Drug_Cst 2)
        box row.GE65_Tot_Day_Suply
        box row.GE65_Bene_Sprsn_Flag
        box row.GE65_Tot_Benes
    |]

let validFipsStates =
    set [
        "AL"; "AK"; "AZ"; "AR"; "CA"; "CO"; "CT"; "DE"; "FL"; "GA"; "HI"; "ID"; "IL"; "IN"; "IA"; "KS"; "KY"; "LA"; "ME"; "MD"; "MA"; "MI"; "MN"; "MS"; "MO"; "MT"; "NE"; "NV"; "NH"; "NJ"; "NM"; "NY"; "NC"; "ND"; "OH"; "OK"; "OR"; "PA"; "RI"; "SC"; "SD"; "TN"; "TX"; "UT"; "VT"; "VA"; "WA"; "WV"; "WI"; "WY"
    ]

let writeParquetFile (filePath: string) (rows: seq<MedicareDataRow>) =
    let schema =
        new GroupNode(
            "schema",
            Repetition.Required,
            [
                new PrimitiveNode("Prscrbr_NPI", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_Last_Org_Name", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_First_Name", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_City", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_State_Abrvtn", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_State_FIPS", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_Type", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Prscrbr_Type_Src", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Brnd_Name", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Gnrc_Name", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("Tot_Clms", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("Tot_30day_Fills", Repetition.Optional, LogicalType.Decimal(9, 2), PhysicalType.ByteArray)
                new PrimitiveNode("Tot_Day_Suply", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("Tot_Drug_Cst", Repetition.Optional, LogicalType.Decimal(9, 2), PhysicalType.ByteArray)
                new PrimitiveNode("Tot_Benes", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("GE65_Sprsn_Flag", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
                new PrimitiveNode("GE65_Tot_Clms", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("GE65_Tot_30day_Fills", Repetition.Optional, LogicalType.Decimal(9, 2), PhysicalType.ByteArray)
                new PrimitiveNode("GE65_Tot_Drug_Cst", Repetition.Optional, LogicalType.Decimal(9, 2), PhysicalType.ByteArray)
                new PrimitiveNode("GE65_Tot_Day_Suply", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("GE65_Tot_Benes", Repetition.Optional, LogicalType.Int(64, true), PhysicalType.Int64)
                new PrimitiveNode("GE65_Bene_Sprsn_Flag", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray)
            ]
        )
        
    let columnArray : Column array =
        [|
            Column<string>("Prscrbr_NPI")
            Column<string>("Prscrbr_Last_Org_Name")
            Column<string>("Prscrbr_First_Name")
            Column<string>("Prscrbr_City")
            Column<string>("Prscrbr_State_Abrvtn")
            Column<string>("Prscrbr_State_FIPS")
            Column<string>("Prscrbr_Type")
            Column<string>("Prscrbr_Type_Src")
            Column<string>("Brnd_Name")
            Column<string>("Gnrc_Name")
            Column<Nullable<int64>>("Tot_Clms")
            Column<Nullable<decimal>>("Tot_30day_Fills", LogicalType.Decimal(precision = 10, scale = 2))
            Column<Nullable<int64>>("Tot_Day_Suply")
            Column<Nullable<decimal>>("Tot_Drug_Cst", LogicalType.Decimal(precision = 10, scale = 2))
            Column<Nullable<int64>>("Tot_Benes")
            Column<string>("GE65_Sprsn_Flag")
            Column<Nullable<int64>>("GE65_Tot_Clms")
            Column<Nullable<decimal>>("GE65_Tot_30day_Fills", LogicalType.Decimal(precision = 10, scale = 2))
            Column<Nullable<decimal>>("GE65_Tot_Drug_Cst", LogicalType.Decimal(precision = 10, scale = 2))
            Column<Nullable<int64>>("GE65_Tot_Day_Suply")
            Column<string>("GE65_Bene_Sprsn_Flag")
            Column<Nullable<int64>>("GE65_Tot_Benes")
            Column<string>("Prscrbr_City_State")
            Column<string>("Prscrbr_Full_Name")
        |]

    use fileWriter = new ParquetFileWriter(filePath, columnArray)
    use rowGroupWriter = fileWriter.AppendRowGroup()

    let writeColumn (columnWriter: LogicalColumnWriter<'T>) (values: 'T[]) =
        columnWriter.WriteBatch(values)

    let chunkRows = rows |> Seq.map rowToBoxedArray |> Seq.toArray

    for i in 0 .. schema.Fields.Length - 1 do
        match rowGroupWriter.NextColumn().LogicalWriter() with
        | :? LogicalColumnWriter<string> as writer -> writeColumn writer (chunkRows |> Array.map (fun row -> unbox<string> row[i]))
        | :? LogicalColumnWriter<int64> as writer -> writeColumn writer (chunkRows |> Array.map (fun row -> unbox<int64> row[i]))
        | :? LogicalColumnWriter<decimal> as writer -> writeColumn writer (chunkRows |> Array.map (fun row -> unbox<decimal> row[i]))
        | :? LogicalColumnWriter<Nullable<int64>> as writer -> writeColumn writer (chunkRows |> Array.map (fun row -> unbox<Nullable<int64>> row[i]))
        | :? LogicalColumnWriter<Nullable<decimal>> as writer -> writeColumn writer (chunkRows |> Array.map (fun row -> unbox<Nullable<decimal>> row[i]))
        | _ -> failwith "Unsupported column type"

    fileWriter.Close()

let splitCsvByState (filePath: string) =
    let csv = loadCsv(filePath)
    let groupedRows =
        csv
        |> Seq.filter (fun row -> validFipsStates.Contains(row.Prscrbr_State_Abrvtn))
        |> Seq.groupBy (fun row -> row.Prscrbr_State_Abrvtn)
        |> Seq.toArray

    for state, rows in groupedRows do
        let stateFilePath = Path.ChangeExtension(filePath, $"_state%s{state}.parquet")
        printfn $"Writing file: %s{stateFilePath}"
        writeParquetFile stateFilePath rows

splitCsvByState "Files/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"