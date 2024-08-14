open System
open System.IO

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

let parseNullableDecimal (s: string) =
    match Decimal.TryParse(s) with
    | true, v ->
        let factor = decimal (Math.Pow(10.0, float 2))
        Nullable<decimal>(Math.Round(v * factor) / factor)
    | _ -> Nullable<decimal>()

let parseCsvRow (line: string) =
    let columns = line.Split(',')
    {
        Prscrbr_NPI = columns.[0]
        Prscrbr_Last_Org_Name = columns.[1]
        Prscrbr_First_Name = columns.[2]
        Prscrbr_City = columns.[3]
        Prscrbr_State_Abrvtn = columns.[4]
        Prscrbr_State_FIPS = columns.[5]
        Prscrbr_Type = columns.[6]
        Prscrbr_Type_Src = columns.[7]
        Brnd_Name = columns.[8]
        Gnrc_Name = columns.[9]
        Tot_Clms = parseNullableInt64 columns.[10]
        Tot_30day_Fills = parseNullableDecimal columns.[11]
        Tot_Day_Suply = parseNullableInt64 columns.[12]
        Tot_Drug_Cst = parseNullableDecimal columns.[13]
        Tot_Benes = parseNullableInt64 columns.[14]
        GE65_Sprsn_Flag = columns.[15]
        GE65_Tot_Clms = parseNullableInt64 columns.[16]
        GE65_Tot_30day_Fills = parseNullableDecimal columns.[17]
        GE65_Tot_Drug_Cst = parseNullableDecimal columns.[18]
        GE65_Tot_Day_Suply = parseNullableInt64 columns.[19]
        GE65_Tot_Benes = parseNullableInt64 columns.[20]
        GE65_Bene_Sprsn_Flag = columns.[21]
    }

let loadCsv (filePath: string) =
    File.ReadLines(filePath)
    |> Seq.skip 1 // Skip header
    |> Seq.map parseCsvRow

let roundNullableDecimal (value: Nullable<decimal>) =
    if value.HasValue then
        let factor = decimal (Math.Pow(10.0, float 2))
        Nullable<decimal>(Math.Round(value.Value * factor) / factor)
    else
        Nullable<decimal>()

let rowToStringArray (row: MedicareDataRow) =
    [|
        row.Prscrbr_NPI
        row.Prscrbr_Last_Org_Name
        row.Prscrbr_First_Name
        row.Prscrbr_City
        row.Prscrbr_State_Abrvtn
        row.Prscrbr_State_FIPS
        row.Prscrbr_Type
        row.Prscrbr_Type_Src
        row.Brnd_Name
        row.Gnrc_Name
        row.Tot_Clms.ToString()
        roundNullableDecimal(row.Tot_30day_Fills).ToString()
        row.Tot_Day_Suply.ToString()
        roundNullableDecimal(row.Tot_Drug_Cst).ToString()
        row.Tot_Benes.ToString()
        row.GE65_Sprsn_Flag
        row.GE65_Tot_Clms.ToString()
        roundNullableDecimal(row.GE65_Tot_30day_Fills).ToString()
        roundNullableDecimal(row.GE65_Tot_Drug_Cst).ToString()
        row.GE65_Tot_Day_Suply.ToString()
        row.GE65_Bene_Sprsn_Flag
        row.GE65_Tot_Benes.ToString()
    |]
    
let validFipsStates = 
    set [
        "AL"; "AK"; "AZ"; "AR"; "CA"; "CO"; "CT"; "DE"; "FL"; "GA"; "HI"; "ID"; "IL"; "IN"; "IA"; "KS"; "KY"; "LA"; "ME"; "MD"; "MA"; "MI"; "MN"; "MS"; "MO"; "MT"; "NE"; "NV"; "NH"; "NJ"; "NM"; "NY"; "NC"; "ND"; "OH"; "OK"; "OR"; "PA"; "RI"; "SC"; "SD"; "TN"; "TX"; "UT"; "VT"; "VA"; "WA"; "WV"; "WI"; "WY"
    ]

let splitCsvByState (filePath: string) =
    let csv = loadCsv(filePath)
    let groupedRows = 
        csv 
        |> Seq.filter (fun row -> validFipsStates.Contains(row.Prscrbr_State_Abrvtn))
        |> Seq.groupBy (fun row -> row.Prscrbr_State_Abrvtn) 
        |> Seq.toArray

    for (state, rows) in groupedRows do
        let stateFilePath = Path.ChangeExtension(filePath, sprintf "_state%s.csv" state)
        printfn "Writing file: %s" stateFilePath
        use writer = new StreamWriter(stateFilePath)
        writer.WriteLine("Prscrbr_NPI,Prscrbr_Last_Org_Name,Prscrbr_First_Name,Prscrbr_City,Prscrbr_State_Abrvtn,Prscrbr_State_FIPS,Prscrbr_Type,Prscrbr_Type_Src,Brnd_Name,Gnrc_Name,Tot_Clms,Tot_30day_Fills,Tot_Day_Suply,Tot_Drug_Cst,Tot_Benes,GE65_Sprsn_Flag,GE65_Tot_Clms,GE65_Tot_30day_Fills,GE65_Tot_Drug_Cst,GE65_Tot_Day_Suply,GE65_Tot_Benes,GE65_Bene_Sprsn_Flag")
        for row in rows do
            writer.WriteLine(String.Join(",", rowToStringArray row))

splitCsvByState "Files/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv"