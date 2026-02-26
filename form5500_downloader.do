*==============================================================================
* form5500_downloader.do
* Downloads Form 5500 data from the U.S. DOL EBSA public database.
*
* Equivalent to the Python/Colab notebook: form5500_downloader.ipynb
* Tested on Stata 16+ (Windows, Mac, Linux)
*
* USAGE
*   1. Edit global BASE_DIR below to your local data folder.
*   2. Run: do form5500_downloader.do
*
* FILES DOWNLOADED PER YEAR (EFAST2, 2009–2023)
*   F_5500          Main form: plan identity, type, participants, plan year
*   F_SCH_H         Schedule H: assets, liabilities, income, expenses
*   F_SCH_R         Schedule R: retirement plan info, contribution rates
*   F_SCH_R_PART1   Schedule R Part 1: employer roster (contributing EINs)
*
* ESTIMATED DOWNLOAD SIZE : ~15–25 GB (all years, all file types)
* ESTIMATED TIME          : 45–90 minutes
*
* NOTE: Colab users should use the Python notebook instead. This script is
*       for running downloads from a local Stata installation.
*==============================================================================

version 16.0
clear all
set more off

*------------------------------------------------------------------------------
* CONFIGURATION — edit BASE_DIR before running
*------------------------------------------------------------------------------

* Where to save all downloaded files.
* Windows example : global BASE_DIR "C:/data/form5500/raw"
* Mac example     : global BASE_DIR "/Users/you/data/form5500/raw"
* Linux example   : global BASE_DIR "/home/you/data/form5500/raw"
global BASE_DIR   "/change/me/form5500/raw"

* DOL URL base (EFAST2 format, 2009+)
global DOL_BASE   "https://askebsa.dol.gov/FOIA%20Files"

* Retry and politeness settings
global MAX_RETRY  3    // number of download attempts per file
global PAUSE_MS   3000 // milliseconds between successful downloads (3 sec)
global RETRY_MS   15000 // milliseconds between retries (15 sec)

*------------------------------------------------------------------------------
* PROGRAMS
*------------------------------------------------------------------------------

* ----------------------------------------------------------------------------
* dl_zip: download a zip file from a URL to a local path.
*   Skips if file already exists.
*   Retries up to $MAX_RETRY times on failure.
*
*   Usage: dl_zip "url" "destpath" "label"
* ----------------------------------------------------------------------------
cap program drop dl_zip
program dl_zip
    args url destpath label

    * Skip if file already exists
    cap confirm file "`destpath'"
    if _rc == 0 {
        di as txt "  [SKIP]  `label' (already downloaded)"
        exit
    }

    * Retry loop
    local ok  = 0
    local att = 1
    while `att' <= $MAX_RETRY & `ok' == 0 {
        di as txt "  [DL `att'/$MAX_RETRY] `label'"
        cap copy "`url'" "`destpath'", replace
        if _rc == 0 {
            local ok = 1
            di as txt "  [OK]    `label'"
        }
        else {
            di as err "  [ERR]   `label' — attempt `att' failed (rc=`_rc')"
            if `att' < $MAX_RETRY  sleep $RETRY_MS
        }
        local att = `att' + 1
    }

    if `ok' == 0  di as err "  [FAIL]  `label' — all `= $MAX_RETRY' attempts failed"
end

* ----------------------------------------------------------------------------
* unzip_zip: unzip a file to an output directory.
*   Uses PowerShell on Windows, unzip on Mac/Linux.
*
*   Usage: unzip_zip "zippath" "outdir" "label"
* ----------------------------------------------------------------------------
cap program drop unzip_zip
program unzip_zip
    args zippath outdir label

    cap mkdir "`outdir'"

    if c(os) == "Windows" {
        shell powershell -NoProfile -Command ^
            "Expand-Archive -LiteralPath '`zippath'' ^
             -DestinationPath '`outdir'' -Force"
    }
    else {
        * Mac or Linux
        shell unzip -oq "`zippath'" -d "`outdir'"
    }

    if _rc != 0  di as err "  [UNZIP FAIL] `label'"
    else         di as txt "  [UNZIP] `label' -> `outdir'"
end

* ----------------------------------------------------------------------------
* write_manifest_row: append one row to an open manifest file handle.
*
*   Usage: write_manifest_row handle year stem status zippath url
* ----------------------------------------------------------------------------
cap program drop write_manifest_row
program write_manifest_row
    args handle year stem status zippath url
    file write `handle' "`year',`stem',`status',`zippath',`url'" _n
end


*==============================================================================
* SECTION 1 — EFAST2 MAIN FILES (2009–2023)
* F_5500, F_SCH_H, F_SCH_R, F_SCH_R_PART1
*==============================================================================

di as txt _newline "{hline 65}"
di as txt "Form 5500 Downloader — EFAST2 Main Files (2009–2023)"
di as txt "{hline 65}"

* Create base directory
cap mkdir "$BASE_DIR"

* Open manifest
local mf_main "$BASE_DIR/download_manifest.csv"
file open mf using "`mf_main'", write replace
file write mf "year,file_type,status,zip_path,url" _n

* File types to download each year
local stems F_5500 F_SCH_H F_SCH_R F_SCH_R_PART1

forvalues yr = 2009/2023 {
    local yd "$BASE_DIR/`yr'"
    cap mkdir "`yd'"

    di as txt _newline "  {hline 50}"
    di as txt "  YEAR `yr'"
    di as txt "  {hline 50}"

    foreach stem of local stems {
        local fname  "`stem'_`yr'_Latest.zip"
        local url    "$DOL_BASE/`yr'/Latest/`fname'"
        local zpath  "`yd'/`fname'"
        local csvdir "`yd'/`stem'"

        dl_zip   "`url'" "`zpath'" "`yr' / `stem'"

        * Record result
        cap confirm file "`zpath'"
        if _rc == 0 {
            unzip_zip "`zpath'" "`csvdir'" "`yr' / `stem'"
            write_manifest_row mf `yr' `stem' OK "`zpath'" "`url'"
        }
        else {
            write_manifest_row mf `yr' `stem' DOWNLOAD_FAIL "`zpath'" "`url'"
        }

        sleep $PAUSE_MS
    }
}

file close mf
di as txt _newline "Manifest written: `mf_main'"


*==============================================================================
* SECTION 2 — EFAST1 EARLY YEARS (1999–2008)
* F_5500 and F_SCH_H only — Schedule R did not exist before 2009.
*
* URL pattern (no "Latest/" subdirectory):
*   askebsa.dol.gov/FOIA Files/{year}/{stem}_{year}.zip
*==============================================================================

di as txt _newline "{hline 65}"
di as txt "Form 5500 Downloader — EFAST1 Early Years (1999–2008)"
di as txt "{hline 65}"
di as txt "NOTE: Schedule R / R Part 1 not available before 2009."

local mf_early "$BASE_DIR/download_manifest_1999_2008.csv"
file open ef using "`mf_early'", write replace
file write ef "year,file_type,status,zip_path,url" _n

local early_stems F_5500 F_SCH_H

forvalues yr = 1999/2008 {
    local yd "$BASE_DIR/`yr'"
    cap mkdir "`yd'"

    di as txt _newline "  {hline 50}"
    di as txt "  YEAR `yr' (EFAST1)"
    di as txt "  {hline 50}"

    foreach stem of local early_stems {
        local fname  "`stem'_`yr'.zip"
        local url    "$DOL_BASE/`yr'/`fname'"
        local zpath  "`yd'/`fname'"
        local csvdir "`yd'/`stem'"

        dl_zip   "`url'" "`zpath'" "`yr' / `stem'"

        cap confirm file "`zpath'"
        if _rc == 0 {
            unzip_zip "`zpath'" "`csvdir'" "`yr' / `stem'"
            write_manifest_row ef `yr' `stem' OK "`zpath'" "`url'"
        }
        else {
            write_manifest_row ef `yr' `stem' DOWNLOAD_FAIL "`zpath'" "`url'"
        }

        sleep $PAUSE_MS
    }
}

file close ef
di as txt _newline "Manifest written: `mf_early'"


*==============================================================================
* SECTION 3 — DATA DICTIONARIES (2009–2023)
* Excel workbooks defining every field in every schedule.
* Essential before writing any cleaning or merging code.
*
* URL: dol.gov/.../form-5500-{year}-data-dictionary.zip
* Contents: one Excel tab per schedule (F_5500, F_SCH_H, F_SCH_R, etc.)
*==============================================================================

di as txt _newline "{hline 65}"
di as txt "Downloading Data Dictionaries (2009–2023)"
di as txt "{hline 65}"

local DICT_BASE "https://www.dol.gov/sites/dolgov/files/EBSA/about-ebsa/our-activities/public-disclosure/foia"
local DICT_DIR  "$BASE_DIR/data_dictionaries"
cap mkdir "`DICT_DIR'"

local mf_dict "$BASE_DIR/dictionary_manifest.csv"
file open df using "`mf_dict'", write replace
file write df "year,status,zip_path,url" _n

forvalues yr = 2009/2023 {
    local yr_dict_dir "`DICT_DIR'/`yr'"
    cap mkdir "`yr_dict_dir'"

    local fname  "form-5500-`yr'-data-dictionary.zip"
    local url    "`DICT_BASE'/`fname'"
    local zpath  "`DICT_DIR'/`fname'"

    dl_zip "`url'" "`zpath'" "Data Dictionary `yr'"

    cap confirm file "`zpath'"
    if _rc == 0 {
        unzip_zip "`zpath'" "`yr_dict_dir'" "Dictionary `yr'"
        file write df "`yr',OK,`zpath',`url'" _n
    }
    else {
        file write df "`yr',DOWNLOAD_FAIL,`zpath',`url'" _n
    }

    sleep $PAUSE_MS
}

file close df
di as txt _newline "Manifest written: `mf_dict'"


*==============================================================================
* SECTION 4 — SCHEDULE A FILES (F_5500_SF, F_SCH_A, F_SCH_A_PART1)
*
* F_5500_SF       Short Form 5500-SF (small plans < 100 participants)
* F_SCH_A         Schedule A: insurance contracts and premiums paid
* F_SCH_A_PART1   Schedule A Part 1: per-carrier detail
*                 Contract type codes:
*                   1 = Health/Medical     5 = Long-term disability
*                   2 = Life insurance     6 = Long-term care
*                   3 = Dental/Vision      7 = Pension/annuity
*                   4 = Temporary disability
*
* Use Part 1 to split pension vs. health & welfare contributions.
*
* EFAST2 (2009–2023): all three files available
* EFAST1 (1999–2008): F_5500_SF and F_SCH_A only (Part 1 availability varies)
*==============================================================================

di as txt _newline "{hline 65}"
di as txt "Downloading Schedule A Files"
di as txt "{hline 65}"

local mf_a "$BASE_DIR/download_manifest_sch_a.csv"
file open af using "`mf_a'", write replace
file write af "year,file_type,status,zip_path,url" _n

* EFAST2 (2009–2023)
local sch_a_stems F_5500_SF F_SCH_A F_SCH_A_PART1

forvalues yr = 2009/2023 {
    local yd "$BASE_DIR/`yr'"
    cap mkdir "`yd'"

    foreach stem of local sch_a_stems {
        local fname  "`stem'_`yr'_Latest.zip"
        local url    "$DOL_BASE/`yr'/Latest/`fname'"
        local zpath  "`yd'/`fname'"
        local csvdir "`yd'/`stem'"

        dl_zip "`url'" "`zpath'" "`yr' / `stem'"

        cap confirm file "`zpath'"
        if _rc == 0 {
            unzip_zip "`zpath'" "`csvdir'" "`yr' / `stem'"
            write_manifest_row af `yr' `stem' OK "`zpath'" "`url'"
        }
        else {
            write_manifest_row af `yr' `stem' DOWNLOAD_FAIL "`zpath'" "`url'"
        }

        sleep $PAUSE_MS
    }
}

* EFAST1 (1999–2008) — F_5500_SF and F_SCH_A only
forvalues yr = 1999/2008 {
    local yd "$BASE_DIR/`yr'"
    cap mkdir "`yd'"

    foreach stem in F_5500_SF F_SCH_A {
        local fname  "`stem'_`yr'.zip"
        local url    "$DOL_BASE/`yr'/`fname'"
        local zpath  "`yd'/`fname'"
        local csvdir "`yd'/`stem'"

        dl_zip "`url'" "`zpath'" "`yr' / `stem'"

        cap confirm file "`zpath'"
        if _rc == 0 {
            unzip_zip "`zpath'" "`csvdir'" "`yr' / `stem'"
            write_manifest_row af `yr' `stem' OK "`zpath'" "`url'"
        }
        else {
            write_manifest_row af `yr' `stem' DOWNLOAD_FAIL "`zpath'" "`url'"
        }

        sleep $PAUSE_MS
    }
}

file close af
di as txt _newline "Manifest written: `mf_a'"


*==============================================================================
* DONE
*==============================================================================

di as txt _newline "{hline 65}"
di as txt "All downloads complete."
di as txt "Files saved to: $BASE_DIR"
di as txt ""
di as txt "Manifests:"
di as txt "  EFAST2 (2009–2023)  : $BASE_DIR/download_manifest.csv"
di as txt "  EFAST1 (1999–2008)  : $BASE_DIR/download_manifest_1999_2008.csv"
di as txt "  Data Dictionaries   : $BASE_DIR/dictionary_manifest.csv"
di as txt "  Schedule A          : $BASE_DIR/download_manifest_sch_a.csv"
di as txt "{hline 65}"
