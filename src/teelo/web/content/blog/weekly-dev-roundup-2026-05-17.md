---
title: Weekly Update — Keeping Live Tournament Data Honest
date: 2026-05-17
author: Cam
excerpt: A quieter week on Teelo, focused on making live tournament dates and current-event predictions less brittle as draws move through the week.
category: Dev
draft: true
---

# Weekly Update — Keeping Live Tournament Data Honest

This was another quiet week on the surface.

No big new page, no dramatic redesign, and no flashy feature to announce. Most of the work was in the slightly awkward middle layer of Teelo: the bit that tries to keep live tournament data lined up while the real world is still changing underneath it.

That sounds dry, and honestly it is a bit dry. But it matters a lot.

## Live tournaments need to stay live

The main focus was tournament timing, especially for WTA events.

Tennis is much easier to model after the fact. Once an event is finished, every date, score, and round is settled. Live tournaments are messier. The source data can improve during the week, a tournament card might expose better dates later than it did at first, and Teelo has to avoid clinging to an early guess once better information turns up.

So I tightened how Teelo handles start and end dates for tournament editions. If the site first creates an event with an estimated end date, then later finds a cleaner one from the calendar data, it can now correct itself instead of carrying the old assumption forward.

From a user point of view, the goal is simple: current tournaments should feel current. Finals should not vanish into a date edge case, and predictions should not look stale because the tournament window was slightly wrong.

## Better date handling in the pipeline

There was also some cleanup around how match dates are estimated from tournament rounds.

One small bug came from mixing plain dates and full datetimes in the same calculation. That is the kind of thing that sounds tiny until it quietly stops a final-round match date from being filled in properly. It is fixed now, with a test covering both mixed-date directions so it should not sneak back in later.

I also added better support for WTA 1000 event lengths, which helps Teelo avoid treating longer events like shorter ones when it has to make an initial estimate.

## Not glamorous, still worth doing

This is not the kind of week where the site suddenly looks different.

But it is the sort of work that makes everything else easier to trust. Predictions, draws, match pages, and tournament views all depend on the underlying event state being sensible.

Still early days, but Teelo is getting a little better at dealing with live tennis being live tennis: awkward dates, moving draws, partial information, and all the small edge cases that only show up once the thing is actually running.