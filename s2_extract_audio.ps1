$RATE = 44100
$TRUNCATE_SECS = 6 * 60
$ARTIFACTS_PATH = "D:\AOR\artifacts"

$folders = Get-ChildItem -Path "$ARTIFACTS_PATH\video" -Directory
$processed_folders = Get-ChildItem -Path "$ARTIFACTS_PATH\audio" -Directory
$folders = $folders | Where-Object { $processed_folders.Name -notcontains $_.Name } | Sort-Object { -([int]$_.Name) }

foreach ($folder in $folders)
{
    $folder_path = $folder.FullName
    $audio_path = "$ARTIFACTS_PATH\audio\_$( $folder.Name )"
    mkdir $audio_path

    $video_files = Get-ChildItem -Path $folder_path -Filter "*.mp4" -File
    foreach ($_ in $video_files)
    {
        Write-Host Processing $_.FullName
        $video_file = $_.FullName
        $audio_file = "$audio_path\$( $_.BaseName ).wav"
        $messages = & ffmpeg -sseof -$TRUNCATE_SECS -i $video_file -threads 8 -ar $RATE -ac 1 -f wav $audio_file -y 2>&1

        # If there is an error, delete the audio file and skip to the next video
        # Also add an entry to errors.txt
        if ($messages -match "Error")
        {
            Write-Host ERROR found. Skipping $video_file
            Remove-Item $audio_file
            Add-Content -Path "$ARTIFACTS_PATH\audio\errors.csv" -Value $( $video_file + ",Errors," + $( $messages -replace '`n', ' ' ) )
            continue
        }

        # If the audio file is too short, delete it and skip to the next video
        # Also add an entry to errors.txt
        $time = $messages `
            | Select-String -Pattern "time=(\d+):(\d+):(\d+)" `
            | Select-Object -Last 1 `
            | ForEach-Object {
            $hours = [int]$_.Matches.Groups[1].Value
            $minutes = [int]$_.Matches.Groups[2].Value
            $seconds = [int]$_.Matches.Groups[3].Value
            $hours * 3600 + $minutes * 60 + $seconds
        }

        $total_duration = $messages `
            | Select-String -Pattern "Duration: (\d+):(\d+):(\d+)" `
            | Select-Object -Last 1 `
            | ForEach-Object {
            $hours = [int]$_.Matches.Groups[1].Value
            $minutes = [int]$_.Matches.Groups[2].Value
            $seconds = [int]$_.Matches.Groups[3].Value
            $hours * 3600 + $minutes * 60 + $seconds
        }

        $new_name = $audio_file -replace ".wav", "_$total_duration.wav"
        Write-Host Renaming $audio_file to $new_name
        Rename-Item -Path $audio_file -NewName $new_name

        if ($time -lt ($TRUNCATE_SECS - 10))
        {
            Write-Host Audio file too short. Skipping $video_file $time
            Remove-Item $audio_file
            Add-Content -Path "$ARTIFACTS_PATH\audio\errors.csv" -Value $( $video_file + ",TooShort" )
            continue
        }
    }

    Rename-Item -Path $audio_path -NewName $folder.Name
}
