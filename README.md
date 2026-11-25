# outlist-scraper

Event crawler for [Outlist](https://outlist.org). It reads the list of event sources from Outlist, crawls each page with a headless
browser (Crawl4AI), extracts events (title, date and time, venue, image, description) and posts them to Outlist's scraping webhook.

It lives in a public repository only so that GitHub Actions minutes are free. It holds **no secrets**: the shared webhook secret is stored
as a repository secret, and the only things this code can do on the Outlist side are the four actions of the `scraper-gateway` function
(is automation on, list sources, record a source outcome, log a run) plus posting events to the webhook.

| File | What it is |
|---|---|
| `scripts/scrapers/outlist_crawler.py` | the crawler (`python scripts/scrapers/outlist_crawler.py`, offline self-check with `--validate`) |
| `scripts/scrapers/partyflock.py` | deprecated stub, not used (Partyflock is parsed by the crawler's `microdata_events` parser) |
| `scripts/scrapers/source_schemas.json` | CSS selector packs per site |
| `scripts/scrapers/fixtures/`, `test_fixtures/` | saved HTML used by the offline checks |
| `.github/workflows/scrape.yml` | scheduled run (08, 12, 16, 18, 20 UTC, +40 min) and manual run |
| `.github/workflows/validate.yml` | offline parser checks on every push |

## Setup (owner)

1. Repository **Settings → Secrets and variables → Actions**: secret `SCRAPER_WEBHOOK_SECRET` (same value as in Supabase) and variable `SUPABASE_URL` (`https://<project-ref>.supabase.co`).
2. **Actions → Scrape events → Run workflow** with `dry_run` ticked to check it end to end without writing; then run it for real.
3. GitHub switches off scheduled workflows after 60 days without repository activity: open the Actions tab now and then, or push a trivial commit.

## Rules

* Never commit keys, tokens or personal data. Selectors and URLs of public pages are the only content.
* Every site's terms and robots.txt apply; do not raise the crawl rate.
* The code is mirrored from the private Outlist repository (`scripts/scrapers/export-public-repo.mjs`): change it there and re-export.

## Instagram scraper

`scripts/scrapers/instagram_scraper.py` and `.github/workflows/instagram-scrape.yml` fetch the newest posts of the Instagram pages an Outlist admin added (Apify, no login, no browser). Outlist decides which posts are events with its own small local model and puts them in the normal approval queue; nothing is published automatically. Extra secret: `APIFY_API_KEY`. Start with **Actions → Scrape Instagram → Run workflow** with `dry_run` ticked (default). Offline check: `python scripts/scrapers/instagram_scraper.py --validate`.

## Primary sources

`scripts/scrapers/primary_sources/` holds one folder per official feed or API (`client.py`, `mapper.py`, `meta.json` with the robots.txt and terms-of-use note and the risk level, `fixtures/`, `test_offline.py`). `.github/workflows/primary-scrape.yml` runs them politely and posts the events with `source_kind=primary`. Sources whose terms forbid bots ship switched off. Dry run: **Actions → Scrape primary sources → Run workflow** (default `dry_run`). Offline check: `python scripts/scrapers/primary_sources/run.py --validate`. New sources are added only with the `/primary-source` protocol of the main repository.
