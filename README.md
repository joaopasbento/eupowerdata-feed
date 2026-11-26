# eupowerdata-feed

The data feed of [eupowerdata.com](https://eupowerdata.com): GitHub Actions fetch the sources (ENTSO-E, OMIE, SMARD, Elexon, Ember and others) and publish what the site reads in `data/`.

## The price history of the Pro plan

The history of the day-ahead prices by day and by interval (each zone's years and the daily means, made by `fetch_price_history.py`) belongs to the Pro plan and is **not in this repository**: since 26 Nov 2026 it is kept in a private storage bucket that only the site reads. What stays here, public, is its index and the monthly means: `data/price-history/index.json` and `data/price-history/monthly.json`.
