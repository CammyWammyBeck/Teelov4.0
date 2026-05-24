---
title: Weekly Update — Making Live Tournament Dates Less Fragile
date: 2026-05-03
author: Cam
excerpt: A quieter week on the surface, but an important one for making live WTA tournaments easier for Teelo to track properly, especially around dates, finals, and current-event updates.
category: Dev
draft: true
---

# Weekly Update — Making Live Tournament Dates Less Fragile

This was a quieter week in terms of shiny user-facing changes, but it still ended up being a useful one.

Most of the work went into one of those slightly invisible parts of Teelo: making sure live tournament data keeps lining up with reality as an event moves through the draw.

That sounds boring until it breaks. Then it becomes very much not boring.

## Better handling for live WTA tournament dates

The main focus this week was WTA tournament scheduling, using Madrid as the real-world test case.

Teelo already tracks tournaments as they move through the week, but current events can be awkward. Sometimes the site has to work with partial information first, then update it later when better source data becomes available. If the original estimate sticks around too stubbornly, Teelo can end up thinking a tournament is further along than it really is.

That is especially annoying near the end of an event, because the final is exactly when you want the draw and predictions to be clean.

I tightened that up so Teelo can now use clearer start and end dates from WTA calendar data when they are available, and correct an existing tournament edition if the later source data is better than the first guess.

From a user point of view, the aim is simple: live tournaments should stay current for longer, and finals should be less likely to disappear into a weird edge case.

## Madrid exposed a useful bug

The Madrid WTA final helped uncover a small but important date bug.

Part of the system estimates match dates from the tournament window and the round being played. That worked fine when the date values were all in the same format, but not when one side was a date and the other was a full datetime. That mismatch could stop final-round scheduling from being filled in properly.

That is fixed now, with a regression test added so the same mixed-date case should not quietly come back later.

## Not every useful week looks flashy

There was not a huge new feature this week, and that is fine.

A lot of Teelo’s usefulness depends on boring things being correct: tournament dates, draw state, match scheduling, and whether the pipeline understands what is actually happening right now.

When those pieces are sturdier, the visible parts of the site become easier to trust — predictions, tournament pages, draw progression, all of it.

So this week was mostly about reducing the number of ways live tournaments can get themselves into an odd state. Not glamorous, but definitely worth doing.

Still early days, but this is the kind of cleanup that makes the whole thing feel more dependable over time.
