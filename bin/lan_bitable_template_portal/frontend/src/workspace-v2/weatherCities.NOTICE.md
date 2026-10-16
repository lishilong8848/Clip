# Weather city dataset source notice

`weatherCities.json` in this directory is a **compact, pre-generated static data
file** bundled for browser-locale weather resolution in the V2 workspace.

## Source

- Data: **QWeather (和风天气) LocationList — `China-City-List-latest.csv`**
- Raw CSV: <https://raw.githubusercontent.com/qwd/LocationList/master/China-City-List-latest.csv>
- Repository: <https://github.com/qwd/LocationList>
- Official docs: <https://dev.qweather.com/docs/resource/location-list/>

## What was generated

Only **mainland China prefecture/province-level city centers** (administrative
division codes ending in `00`, excluding 台湾/香港/澳门) are kept. Each entry is:

```
[province, resolvableCityName, latitude, longitude]
```

- Latitudes/longitudes are the prefecture center coordinates as published in the CSV.
- `resolvableCityName` is the name passed to the existing Chinaweather endpoint:
  the bare prefecture name where unique, otherwise `province+city` (for example
  `辽宁省朝阳`) so that an ambiguous district sharing the same name in another
  province is never chosen.

This file is used **only as a bundled static lookup** to approximate the nearest
prefecture center from browser-granted coordinates. It is **never transmitted
anywhere** — no runtime foreign service receives the user's coordinates.

## Accuracy

A resolved city is an **approximate locality**: the nearest prefecture center,
not an exact administrative boundary. Resolutions farther than `MAX_DISTANCE_KM`
(100 km) from every known center are rejected and fall back to a configured city.

## License attribution

The upstream `qwd/LocationList` repository at the time of generation does not
ship a LICENSE file in its tree and exposes no license metadata via the GitHub
API. We therefore retain the following attribution in `weatherCities.json`'s
`_source` / `_source_url` / `_docs` fields and in this notice:

- Source of coordinates and names: QWeather LocationList (`China-City-List-latest.csv`).
- Generated offline (data acquisition), not a runtime foreign call.
- Please refer to the upstream repository for the authoritative current list and
  any future licensing terms.

<generated-utc>2026-10-15</generated-utc>