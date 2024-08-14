# Define the input and output file paths
$inputFile = "D:\CMS\parquet\Files\Medicare_Part_D_Prescribers_by_Provider_and_Drug_2013.csv"
$outputFile = "D:\CMS\parquet\sample.csv"

try {
    # Initialize StreamReader to read the input file
    $reader = [System.IO.StreamReader]::new($inputFile)
    if (-not $reader) {
        throw "Failed to initialize StreamReader."
    }

    # Initialize StreamWriter to write to the output file
    $writer = [System.IO.StreamWriter]::new($outputFile)
    if (-not $writer) {
        throw "Failed to initialize StreamWriter."
    }

    # Read and write the first 1000 rows (including the header)
    for ($i = 0; $i -lt 1001; $i++) {
        $line = $reader.ReadLine()
        if ($line -eq $null) {
            throw "Unexpected end of file while reading the first 1000 rows."
        }
        $writer.WriteLine($line)
    }

    # Function to skip a random number of lines
    function Skip-RandomLines {
        param (
            [int]$min,
            [int]$max
        )
        $skipCount = Get-Random -Minimum $min -Maximum $max
        Write-Host "Skipping $skipCount lines"
        for ($i = 0; $i -lt $skipCount; $i++) {
            $reader.ReadLine() | Out-Null
        }
    }

    # Read and write 10 chunks of 1000 rows each
    for ($chunk = 0; $chunk -lt 10; $chunk++) {
        Skip-RandomLines -min 15000 -max 20000
        for ($i = 0; $i -lt 1000; $i++) {
            $line = $reader.ReadLine()
            if ($line -eq $null) {
                Write-Host "Reached end of file before completing 1000 lines for chunk $chunk."
                break
            }
            $writer.WriteLine($line)
        }
    }
} catch {
    Write-Host "An error occurred: $_"
} finally {
    # Close the StreamReader and StreamWriter
    if ($reader) { $reader.Close() }
    if ($writer) { $writer.Close() }
}