# squirreling away data like acorns

These are scripts for quickly stashing a few federal datasets before they're purged. Mostly based on what datasets I use for work, mostly R scripts I threw together quickly. Some of the scripts aren't great but they get the job done. I only study 2 states for my jobs but most of these I just pulled nationally.

When necessary / easy enough, many of these scripts run in parallel. Lots of files, esp. rasters or record-level data, will be upwards of a couple hundred MB to a gig.

| directory  | dataset name                                                                                                                                                                 | status                                                                          |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| cdc_places | CDC PLACES project, local estimates of public health data. Multiple years & geographies                                                                                      |                                                                                 |
| census     | All ACS 2023 5-year estimate detailed tables, all 2020 decennial tables, CT & MD only (change abbrevs in the script)                                                         |                                                                                 |
| ed_ocr     | Many years of Dept of Education Office of Civil Rights data                                                                                                                  |                                                                                 |
| hmda       | Record-level mortgage application data, several years                                                                                                                        |                                                                                 |
| landcover  | Landcover raster files back to 1975                                                                                                                                          |                                                                                 |
| pulse_pums | Census Bureau's Household Pulse Survey, most waves. Only set of scripts I wrote before the purge, so might need some edits. Builds a duckdb database                         |                                                                                 |
| tiger      | Census Bureau's TIGER shapefiles, many geographies & years                                                                                                                   |                                                                                 |
| wetlands   | NOAA shapefiles (rasters?) of sea level rise impacts on wetlands                                                                                                             |                                                                                 |
| yrbs       | Youth Risk Behavior Survey, many years. One of the first datasets purged bc it includes samples of trans teenagers in recent years. This scrapes a Wayback Machine snapshot. |                                                                                 |
| ejscreen   | Environmental justice indexes from EPA. Scales environmental hazards based on socially vulnerable populations. Tracts & block groups, several years, whole country.          | Already missing web link, now removed from server. Available on Wayback Machine |
| fema       | Spatial data from FEMA's ArcGIS portal, including copies already removed from CDC portal. Fairly large and random.                                                           |                                                                                 |
| hud        | HUD's Picture of Subsidized Housing, point-in-time homelessness counts, fair market rents, income levels                                                                                                    |                                                                                 |
| cdc_bulk   | Batch of data from CDC data portal based on keywords (mostly things mentioning race/ethnicity, gender, environment). Sorted by publishing agency                             | Some datasets are already gone, such as YRBS                                                                                |

There are some utilities in the script `utils.R`.

I'll add to this as I go. Please fork, holler at the [Archive Team](https://github.com/ArchiveTeam/usgovernment-grab), and get into local mutual aid.

"All you fascists bound to lose" --Woody Guthrie
