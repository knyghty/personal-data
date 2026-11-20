# Ideas

Ideas for the pipeline that nobody built yet. If an idea ships or the owner drops it, remove it from this file.

## Proactive nudger

A scheduled agent reads personal-data and Home Assistant, then sends one notification or nothing. Silence is the default. A log keeps each nudge and whether the owner acted on it or dismissed it. The log goes back into the prompt, so the agent does not repeat itself and learns what matters.

## Home cameras

Local-only cameras give the nudger snapshots of the rooms, for example clothes on the floor while power is cheap. Images go to the model and nowhere else. Only the text of a nudge goes into `raw/`. Parked on 2026-11-15 until the owner has the hardware.

## What moves sleep

A weekly report tests which evening signals go with bad nights. Candidates are late screen time, late Claude Code sessions, Albert Heijn receipts, bedroom climate, steps and late meetings. The report gives effect sizes. If the data is too thin, the report says so.

## Grocery run-out

Albert Heijn receipts show how often the owner buys each product. Predict the day each product runs out. Merge that with the offers watchlist into a shopping list. If a discount comes before the run-out day, the list waits for it.

## Money watchdog

Watch bunq for subscriptions that got more expensive, charges that come back after a cancellation, and spending that is unusual for that month.

## What was I doing

Rebuild any date from location, calendar, music, notes and other sources into one view of the day.
