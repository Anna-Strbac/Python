Write-Host "Script is starting..."

# Define the project path
$projectPath = "C:\Users\46704\Desktop\Kunskapskontroll 2 Python\Project"
Write-Host "Project path: $projectPath"

# Set the working directory
Set-Location $projectPath
Write-Host "Changed directory to project path."

# Construct the path for the tests directory
$testPath = Join-Path $projectPath "tests"
Write-Host "Test path: $testPath"

# Check if the tests directory exists
if (-not (Test-Path $testPath)) {
    Write-Host "Test directory not found: $testPath"
    exit 1
}

# Create a FileSystemWatcher to monitor changes in the project folder
$watcher = New-Object System.IO.FileSystemWatcher
$watcher.Path = $projectPath
$watcher.IncludeSubdirectories = $true
$watcher.EnableRaisingEvents = $true
$watcher.Filter = "*.py"  # Monitor only .py files

# Register an event handler for the "Changed" event
$event = Register-ObjectEvent $watcher "Changed" -Action {
    Write-Host "Changes detected, running tests..."
    try {
        # Ensure pytest is available in the system path
        if (-not (Get-Command pytest -ErrorAction SilentlyContinue)) {
            Write-Host "pytest is not installed or not found in the system path."
            return
        }

        # Debugging: Output the test path
        Write-Host "Running pytest on: $testPath"

        # Run pytest and capture the output
        $result = & pytest $testPath --verbose 2>&1  # Capture both stdout and stderr
        
        # Display the result of the tests
        Write-Host "Test Results:"
        Write-Host $result
        
        # Log the results to a file
        $logFile = Join-Path $projectPath "test_results.log"
        Add-Content $logFile "Test run on $(Get-Date):"
        Add-Content $logFile $result
        Add-Content $logFile "-------------------------------`n"
        Write-Host "Log file updated: $logFile"

    } catch {
        Write-Host "An error occurred while running pytest:"
        Write-Host $_.Exception.Message
    }
}

# Start an infinite loop to keep the script running and watching for events
try {
    while ($true) {
        Write-Host "Watching for changes..."
        
        # Wait for a file system event to occur (file change)
        Wait-Event

        # This will wait indefinitely for any registered event
        # and will allow the above action to execute whenever a change is detected
    }
} finally {
    # Clean up: unregister the event when done (although it never exits in this loop)
    Unregister-Event -SourceIdentifier $event.SourceIdentifier
    $watcher.Dispose()  # Clean up the FileSystemWatcher
}