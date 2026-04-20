---
title: Weekly Update — Faster Forecasts and More Reliable Tournament Data
date: 2026-04-19
author: Cam
excerpt: This week was less about flashy new pages and more about making Teelo’s tournament data and forecasts faster, cleaner, and a lot less likely to get tripped up by messy source data.
category: Dev
draft: true
---

# Weekly Update — Faster Forecasts and More Reliable Tournament Data

This week was a bit less flashy on the surface, but it was still a useful one.

A lot of the work went into making Teelo more reliable behind the scenes, especially around tournament forecasts and scraped match data. That kind of work is not always the most exciting to announce, but it matters a lot if the goal is to make the site something you can actually trust day to day.

## Tournament forecasts should feel snappier

After getting tournament forecasts live, the next job was making sure they stayed practical as draws got bigger.

I tightened up the way forecast data is read so Teelo is only loading the bits it actually needs, instead of dragging in a lot of extra state every time it builds the probabilities view. In plain English: forecast pages should be lighter and quicker, especially when a tournament has a lot going on.

That is one of those changes users might not notice directly, but they would definitely notice if it was not there.

## Hourly updates are doing less useless work

I also fixed an issue in the hourly discovery pipeline where Teelo was looking way too far ahead when checking tournaments.

That sounds small, but it meant the system could end up creating a pile of future work it did not actually need yet. Narrowing that window back down should make the update pipeline more focused, so it spends less time shuffling future tournaments around and more time dealing with the events that actually matter now.

## Better protection against bad tournament data

The other big theme this week was cleaning up edge cases in ATP and WTA scraping.

On the ATP side, I fixed a surface fallback issue so Teelo is less likely to quietly assume the wrong surface when the source data is incomplete. That matters because surface context is a big part of how tennis matches should be interpreted.

On the WTA side, I fixed a more annoying bug: some not-yet-finished matches could look enough like completed matches in the raw page structure that they were being treated as finished when they were not. That is now gated much more carefully, so Teelo should be less likely to ingest nonsense results from upcoming or live matches.

That is exactly the kind of bug I would rather catch and kill early, because if the raw match state is wrong, everything built on top of it gets shaky fast.

## Summary

So this week was mostly about sturdiness.

No huge shiny new feature, but Teelo’s tournament forecasts should be faster, the hourly pipeline should be more efficient, and the ATP/WTA data handling should be a lot safer around awkward edge cases.

It is the sort of work that makes the product feel more solid over time. Still early days, but this is the stuff that helps Teelo earn trust rather than just add features for the sake of it.