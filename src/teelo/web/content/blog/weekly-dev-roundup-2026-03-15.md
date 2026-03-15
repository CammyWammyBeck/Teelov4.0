---
title: Weekly Update — Predictions, Dark Mode, and a New Tournaments Page
date: 2026-03-15
author: Cammy
excerpt: Match predictions are live on the site, dark mode has landed, there is a new tournaments browse page, and the home page has been redesigned.
category: Dev
draft: false
---

# Weekly Update — Predictions, Dark Mode, and a New Tournaments Page

This has been a big week for Teelo. The headline is that match predictions are now live on the site, but there is plenty more besides.

## Match predictions are live

The biggest update by far. Teelo now shows win probability predictions for upcoming matches, and you can click into any match to see a full breakdown of the factors behind each prediction.

I have written a [dedicated blog post](/blog/match-predictions-are-live) about this since there is a lot to cover. The short version: predictions appear in match tables, there is a new match detail page with a prediction hero and feature comparison, and the model behind it all has been substantially upgraded.

## Dark mode

Teelo now respects your system theme preference. If your device is set to dark mode, the site will follow automatically.

This is not a toggle you need to find in settings — it just works. Every page, every component, and every colour has been updated to look right in both light and dark mode.

## Tournaments browse page

There is now a proper page for browsing tournaments. You can filter by tour (ATP, WTA, Challenger, ITF) and see all current and upcoming events in one place.

Previously, the only way to find a tournament was through search or by stumbling across it in a match table. Now there is a dedicated starting point for exploring the tour calendar.

## Home page redesign

The home page has been reworked with a two-column dashboard layout. Match tables now span the full width for easier scanning, and the blog section sits alongside them in the right column.

It should feel more like a dashboard and less like a list of things stacked on top of each other.

## Under the hood

A few things that are not immediately visible but worth mentioning:

- The prediction model was upgraded to "baseline v2" with new feature groups covering opponent quality, dominance, fatigue, and tournament history
- A prediction pipeline now runs automatically as part of the hourly update cycle
- Tournament bracket navigation was redesigned with round-based navigation and CSS Grid
- Various performance improvements to home page and matches page queries

## Summary

Predictions are the centrepiece of this update — Teelo can now tell you what it thinks is going to happen, and show you why. Dark mode, the tournaments page, and the home page redesign round out a week that has moved the site forward significantly.
