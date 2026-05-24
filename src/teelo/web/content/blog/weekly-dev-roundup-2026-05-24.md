---
title: Weekly Update — Making Tournament Forecasts Less Weird
date: 2026-05-24
author: Cam
excerpt: A week of cleanup around tournament forecasts, incomplete Grand Slam draws, stale predictions, and ATP schedules so live event pages behave more sensibly.
category: Dev
draft: true
---

# Weekly Update — Making Tournament Forecasts Less Weird

This week was mostly about tournament pages behaving properly when tennis data gets messy.

Which, to be fair, is often.

The main focus was tournament forecasts: the little title-probability view that tries to show how a draw is shaping up before every match has been played. It is one of the more interesting parts of Teelo, but it also depends on the draw structure being clean. If the underlying draw has duplicate slots, cancelled placeholder matches, missing first-round rows, or stale forecast data, the final number can look more confident than it should.

So this week was less about adding a new shiny thing and more about making that existing thing harder to fool.

## Cleaner forecast draws

One useful fix was around duplicate draw slots.

Live tournament data can leave behind old rows when a match changes, a qualifier is resolved, or a cancelled placeholder gets replaced by the real matchup. If Teelo reads both the old and new rows as live parts of the draw, the forecast can end up seeing the same draw position twice.

That is obviously not ideal.

The forecast builder now filters those out more carefully and keeps the active matchup for each slot. From a user point of view, the aim is simple: when you open a tournament forecast, the draw should line up with what is actually happening, not with a half-cleaned scrape history.

I also added more test coverage around this, because bracket bugs are exactly the kind of thing that look fine until one awkward tournament exposes them.

## Waiting for Grand Slam draws to be ready

Grand Slams got a bit of extra caution too.

Because the draws are bigger, partial data is more dangerous. A half-loaded first round can make a forecast look available before Teelo has enough of the bracket to say anything useful.

So forecasts now stay hidden for incomplete Grand Slam draws instead of trying to fill in the gaps too early. That is a small change visually, but it is the right trade-off. No forecast is better than a forecast built from a draw that has not properly arrived yet.

There was also a related tweak so tournament pages can refresh stale forecasts on demand and read existing forecasts properly before trying to rebuild them. The boring version: fewer stale numbers. The useful version: current tournament pages should feel a bit more current.

## ATP schedule data

Another fix landed for ATP schedule ingestion.

The ATP site changed just enough around player links that Teelo was failing to grab some player IDs from scheduled fixtures. That meant court, time, and scheduled-date data could fail to attach to ATP and Challenger matches.

That path is fixed now, so upcoming match rows should have a better chance of showing the useful scheduling detail people actually look for.

## Small polish, less breakage

There was also some web template cleanup around static assets and framework compatibility. Nothing dramatic, but it helps keep logos, favicons, CSS, and rendered pages predictable.

So, another infrastructure-heavy week, but a useful one. Tournament forecasts are a bit less brittle, incomplete draws are handled more honestly, and ATP scheduling should be cleaner.

Still early days, but this is the kind of work that makes Teelo feel less like a demo and more like a site you can actually trust during a live tournament.
