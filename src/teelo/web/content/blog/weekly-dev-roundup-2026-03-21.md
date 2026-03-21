---
title: Weekly Update — Match Times, Prediction Bars, and a Better Matches Page
date: 2026-03-21
author: Cammy
excerpt: Match times now show in your local timezone, predictions have a new visual design, and the matches page has been overhauled.
category: Dev
draft: false
---

# Weekly Update — Match Times, Prediction Bars, and a Better Matches Page

This week has been about polish. The big features from last week — predictions, dark mode, the tournaments page — are all still here. This update is about making them work better and look sharper.

## Match times in your local timezone

This is probably the most useful change this week. Match rows across the site now show scheduled start times, converted automatically to your local timezone.

If you are in London looking at matches in Indian Wells, or in Sydney checking the European clay season schedule, the times you see are the times on your clock. No more mental arithmetic.

This works everywhere matches appear: the home page, the matches page, tournament pages, and player pages.

## Prediction bars

The prediction column in match tables has had a visual upgrade. Instead of plain percentages, predictions now display as a split bar showing each player's win probability at a glance.

It is a small change, but it makes scanning through a list of matches much quicker. You can immediately see which matches are expected to be close and which ones look lopsided.

## Matches page overhaul

The matches page has been properly reworked. Filters are more reliable, the page loads faster, and the overall experience should feel much smoother.

Match rows now also display richer information — circuit badges, ELO ratings, and clearer winner highlighting on completed matches. It is closer to the kind of match browsing experience I want Teelo to have.

## More matches covered

Teelo now picks up WTA qualifying matches that were previously missed. If a match appears on the schedule but is not yet in the draw, the system will now pull it in automatically. This means better coverage of the early rounds at WTA events.

## Under the hood

A few things that are less visible but made a real difference this week:

- ELO ratings now display on all upcoming matches, not just players who have played recently
- Prediction accuracy has improved after fixing several edge cases where player data could get mixed up during draw updates
- The home page now caps blog posts to three on mobile for a cleaner layout

## Summary

This was a week of making things feel right. Match times, better prediction visuals, a faster matches page, and wider match coverage all add up to a noticeably better experience across the site.
