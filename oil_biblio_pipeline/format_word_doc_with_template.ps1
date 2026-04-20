param(
    [Parameter(Mandatory = $true)]
    [string]$TargetPath,
    [string]$BackupPath = "",
    [switch]$CreateBackup
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

function New-Text {
    param(
        [Parameter(Mandatory = $true)]
        [int[]]$Codes
    )

    return -join ($Codes | ForEach-Object { [char]$_ })
}

function Get-PlainText {
    param(
        [Parameter(Mandatory = $true)]
        $Paragraph
    )

    return $Paragraph.Range.Text.Replace("`r", "").Replace("`a", "").Trim()
}

function Set-ParagraphText {
    param(
        [Parameter(Mandatory = $true)]
        $Paragraph,
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    $Paragraph.Range.Text = $Text + "`r"
}

function Hide-PlaceholderSlash {
    param(
        [Parameter(Mandatory = $true)]
        $Paragraph
    )

    if ($Paragraph.Range.Characters.Count -lt 1) {
        return
    }

    $firstCharacter = $Paragraph.Range.Characters.Item(1)
    if ($firstCharacter.Text -eq "/") {
        $firstCharacter.Font.Hidden = -1
        $firstCharacter.Font.Size = 1
        $firstCharacter.Font.Color = 16777215
    }
}

function Set-ParagraphStyle {
    param(
        [Parameter(Mandatory = $true)]
        $Paragraph,
        [Parameter(Mandatory = $true)]
        [int]$StyleId
    )

    $Paragraph.Range.Style = $StyleId
}

function Set-ParagraphFormat {
    param(
        [Parameter(Mandatory = $true)]
        $Paragraph,
        [string]$FontFarEast,
        [string]$FontAscii,
        [double]$FontSize,
        [int]$Bold = 0,
        [int]$Alignment = 3,
        [double]$FirstLineIndentCm = 0.85,
        [double]$SpaceBefore = 0,
        [double]$SpaceAfter = 0,
        [ValidateSet("body", "onehalf", "formula")]
        [string]$LineMode = "body"
    )

    $range = $Paragraph.Range
    if ($FontFarEast) {
        $range.Font.NameFarEast = $FontFarEast
    }
    if ($FontAscii) {
        $range.Font.NameAscii = $FontAscii
        $range.Font.NameOther = $FontAscii
    }
    $range.Font.Size = $FontSize
    $range.Font.Bold = $Bold

    $Paragraph.Alignment = $Alignment
    $Paragraph.FirstLineIndent = $FirstLineIndentCm * 28.35
    $Paragraph.SpaceBefore = $SpaceBefore
    $Paragraph.SpaceAfter = $SpaceAfter

    switch ($LineMode) {
        "onehalf" {
            $Paragraph.LineSpacingRule = 1
        }
        "formula" {
            $Paragraph.LineSpacingRule = 5
            $Paragraph.LineSpacing = 15
        }
        default {
            $Paragraph.LineSpacingRule = 5
            $Paragraph.LineSpacing = 15
        }
    }
}

function Convert-ChapterHeading {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [hashtable]$ChapterNumberMap,
        [Parameter(Mandatory = $true)]
        [string]$ChapterPrefix,
        [Parameter(Mandatory = $true)]
        [string]$ChapterSuffix,
        [Parameter(Mandatory = $true)]
        [string]$ChapterDigitClass
    )

    $pattern = "^{0}([{1}]+){2}\s*(.*)$" -f [regex]::Escape($ChapterPrefix), $ChapterDigitClass, [regex]::Escape($ChapterSuffix)
    $match = [regex]::Match($Text, $pattern)
    if (-not $match.Success) {
        return $null
    }

    $chapterCn = $match.Groups[1].Value
    if (-not $ChapterNumberMap.ContainsKey($chapterCn)) {
        return $null
    }

    $rest = $match.Groups[2].Value.Trim()
    $converted = "{0}{1}{2}" -f $ChapterPrefix, $ChapterNumberMap[$chapterCn], $ChapterSuffix
    if ($rest) {
        $converted += "  " + $rest
    }
    return $converted
}

$songTi = "SimSun"
$heiTi = "SimHei"
$times = "Times New Roman"

$txtAbstract = New-Text 0x6458, 0x8981
$txtAbstractHeading = (New-Text 0x6458) + "    " + (New-Text 0x8981)
$txtReferences = New-Text 0x53C2, 0x8003, 0x6587, 0x732E
$txtReferencesHeading = (New-Text 0x53C2) + " " + (New-Text 0x8003) + " " + (New-Text 0x6587) + " " + (New-Text 0x732E)
$txtAcknowledgement = New-Text 0x81F4, 0x8C22
$txtAcknowledgementHeading = (New-Text 0x81F4) + "    " + (New-Text 0x8C22)
$txtKeywords = New-Text 0x5173, 0x952E, 0x8BCD, 0xFF1A
$txtFigure = New-Text 0x56FE
$txtTable = New-Text 0x8868
$txtSource = New-Text 0x8D44, 0x6599, 0x6765, 0x6E90, 0xFF1A
$txtChapterPrefix = New-Text 0x7B2C
$txtChapterSuffix = New-Text 0x7AE0
$txtEnglishKeywords = "Key Words" + [char]0xFF1A
$chapterDigitClass = New-Text 0x4E00, 0x4E8C, 0x4E09, 0x56DB, 0x4E94, 0x516D, 0x4E03, 0x516B, 0x4E5D, 0x5341

$styleNormal = -1
$styleHeading1 = -2
$styleHeading2 = -3
$styleHeading3 = -4

$chapterNumberMap = @{
    (New-Text 0x4E00) = 1
    (New-Text 0x4E8C) = 2
    (New-Text 0x4E09) = 3
    (New-Text 0x56DB) = 4
    (New-Text 0x4E94) = 5
    (New-Text 0x516D) = 6
    (New-Text 0x4E03) = 7
    (New-Text 0x516B) = 8
    (New-Text 0x4E5D) = 9
    (New-Text 0x5341) = 10
}

if (-not (Test-Path -LiteralPath $TargetPath)) {
    throw "Target file not found: $TargetPath"
}

$resolvedTarget = (Get-Item -LiteralPath $TargetPath).FullName

if ($CreateBackup) {
    if (-not $BackupPath) {
        $item = Get-Item -LiteralPath $resolvedTarget
        $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $BackupPath = Join-Path $item.DirectoryName ($item.BaseName + "_format_backup_" + $stamp + $item.Extension)
    }
    Copy-Item -LiteralPath $resolvedTarget -Destination $BackupPath -Force
}

$word = $null
$doc = $null

try {
    $word = New-Object -ComObject Word.Application
    $word.Visible = $false
    $word.DisplayAlerts = 0

    $doc = $word.Documents.Open($resolvedTarget, $false, $false)

    foreach ($section in $doc.Sections) {
        $ps = $section.PageSetup
        $ps.TopMargin = 3 * 28.35
        $ps.BottomMargin = 3 * 28.35
        $ps.LeftMargin = 3 * 28.35
        $ps.RightMargin = 3 * 28.35
        $ps.HeaderDistance = 2 * 28.35
        $ps.FooterDistance = 2 * 28.35
        $ps.DifferentFirstPageHeaderFooter = $false
    }

    $firstNonEmptyHandled = $false
    $inEnglishAbstract = $false
    $inReferences = $false

    for ($i = 1; $i -le $doc.Paragraphs.Count; $i++) {
        $paragraph = $doc.Paragraphs.Item($i)
        $text = Get-PlainText -Paragraph $paragraph
        $hasInlineShape = $paragraph.Range.InlineShapes.Count -gt 0

        if (-not $text -and -not $hasInlineShape) {
            continue
        }

        if (-not $firstNonEmptyHandled -and $text) {
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 16 -Bold -1 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 18 -LineMode onehalf
            $firstNonEmptyHandled = $true
            continue
        }

        if ($hasInlineShape) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 12 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 6 -SpaceAfter 6 -LineMode body
            if ($text -eq "/") {
                Hide-PlaceholderSlash -Paragraph $paragraph
            }
            continue
        }

        $convertedChapter = Convert-ChapterHeading -Text $text -ChapterNumberMap $chapterNumberMap -ChapterPrefix $txtChapterPrefix -ChapterSuffix $txtChapterSuffix -ChapterDigitClass $chapterDigitClass
        if ($convertedChapter) {
            Set-ParagraphText -Paragraph $paragraph -Text $convertedChapter
            $paragraph = $doc.Paragraphs.Item($i)
            $text = Get-PlainText -Paragraph $paragraph
        }

        switch -Exact ($text) {
            $txtAbstract {
                Set-ParagraphText -Paragraph $paragraph -Text $txtAbstractHeading
                $paragraph = $doc.Paragraphs.Item($i)
                $text = Get-PlainText -Paragraph $paragraph
                break
            }
            $txtReferences {
                Set-ParagraphText -Paragraph $paragraph -Text $txtReferencesHeading
                $paragraph = $doc.Paragraphs.Item($i)
                $text = Get-PlainText -Paragraph $paragraph
                break
            }
            $txtAcknowledgement {
                Set-ParagraphText -Paragraph $paragraph -Text $txtAcknowledgementHeading
                $paragraph = $doc.Paragraphs.Item($i)
                $text = Get-PlainText -Paragraph $paragraph
                break
            }
            "Abstract" {
                Set-ParagraphText -Paragraph $paragraph -Text "ABSTRACT"
                $paragraph = $doc.Paragraphs.Item($i)
                $text = Get-PlainText -Paragraph $paragraph
                break
            }
        }

        if ($text -match "^(?i)keywords:") {
            $newText = [regex]::Replace($text, "^(?i)keywords:", $txtEnglishKeywords)
            Set-ParagraphText -Paragraph $paragraph -Text $newText
            $paragraph = $doc.Paragraphs.Item($i)
            $text = Get-PlainText -Paragraph $paragraph
        }

        if (
            ($text -eq $txtAbstractHeading) -or
            ($text -eq "ABSTRACT") -or
            ($text -eq $txtReferencesHeading) -or
            ($text -eq $txtAcknowledgementHeading) -or
            ($text -match ("^{0}\d+{1}" -f [regex]::Escape($txtChapterPrefix), [regex]::Escape($txtChapterSuffix)))
        ) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleHeading1
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 15 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 11 -LineMode onehalf
            if ($text -eq "ABSTRACT") {
                $paragraph.Range.Font.Bold = -1
                $inEnglishAbstract = $true
                $inReferences = $false
            }
            elseif ($text -eq $txtReferencesHeading) {
                $inEnglishAbstract = $false
                $inReferences = $true
            }
            else {
                $inEnglishAbstract = $false
                $inReferences = $false
            }
            continue
        }

        if ($text -match "^\d+\.\d+\.\d+(\s|$)") {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleHeading3
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 12 -Bold 0 -Alignment 3 -FirstLineIndentCm 0 -SpaceBefore 8.15 -SpaceAfter 0 -LineMode onehalf
            $inEnglishAbstract = $false
            continue
        }

        if ($text -match "^\d+\.\d+(\s|$)") {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleHeading2
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 14 -Bold 0 -Alignment 3 -FirstLineIndentCm 0 -SpaceBefore 8.15 -SpaceAfter 0 -LineMode onehalf
            $inEnglishAbstract = $false
            continue
        }

        if ($text -match ("^{0}\s*\d+[-\.]\d+" -f [regex]::Escape($txtFigure))) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 10.5 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        if ($text -match ("^{0}\s*\d+[-\.]\d+" -f [regex]::Escape($txtTable))) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 10.5 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        if ($text -match "^Table\s+\d") {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 10.5 -Bold -1 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        if ($text.StartsWith($txtSource)) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 10.5 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        if ($text.StartsWith($txtKeywords)) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $heiTi -FontAscii $times -FontSize 12 -Bold -1 -Alignment 3 -FirstLineIndentCm 0 -SpaceBefore 15 -SpaceAfter 0 -LineMode body
            $inEnglishAbstract = $false
            continue
        }

        if ($text.StartsWith($txtEnglishKeywords)) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 12 -Bold -1 -Alignment 3 -FirstLineIndentCm 0 -SpaceBefore 15 -SpaceAfter 0 -LineMode body
            $inEnglishAbstract = $false
            continue
        }

        if ($text -match '^\$\$.*\$\$$') {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 12 -Bold 0 -Alignment 1 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode formula
            continue
        }

        if ($inReferences) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 10.5 -Bold 0 -Alignment 0 -FirstLineIndentCm 0 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        if ($inEnglishAbstract) {
            Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
            Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 12 -Bold 0 -Alignment 3 -FirstLineIndentCm 0.85 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
            continue
        }

        Set-ParagraphStyle -Paragraph $paragraph -StyleId $styleNormal
        Set-ParagraphFormat -Paragraph $paragraph -FontFarEast $songTi -FontAscii $times -FontSize 12 -Bold 0 -Alignment 3 -FirstLineIndentCm 0.85 -SpaceBefore 0 -SpaceAfter 0 -LineMode body
    }

    $doc.Save()
    Write-Output ("formatted=" + $resolvedTarget)
    if ($CreateBackup) {
        Write-Output ("backup=" + $BackupPath)
    }
}
finally {
    if ($doc -ne $null) {
        $doc.Close()
        [System.Runtime.Interopservices.Marshal]::ReleaseComObject($doc) | Out-Null
    }
    if ($word -ne $null) {
        $word.Quit()
        [System.Runtime.Interopservices.Marshal]::ReleaseComObject($word) | Out-Null
    }
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}
