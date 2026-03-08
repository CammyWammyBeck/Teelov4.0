---
title: Weekly Update — Search, Tournament Pages, and Prediction Progress
date: 2026-03-08
author: Cammy
excerpt: Global search is live, tournament pages now have brackets and match tabs, and prediction work has properly started behind the scenes.
category: Dev
draft: false
---

# Weekly Update — Search, Tournament Pages, and Prediction Progress

A lot landed on Teelo over the last couple of days.

This update is a bit more front-end heavy than usual, which is good news if you are actually using the site.

## Global search is live

There is now a proper search bar in the site navigation.

You can use it to search for players and tournaments, either as a quick jump from the dropdown or through the full search results page.

If you know roughly what you want, you should now be able to get there without digging through rankings pages or match tables.

## Tournament pages are now much more useful

Tournaments now have their own proper pages, with a lot more context in one place.

Each tournament page now gives you:

- a tournament header with the key details
- a matches tab for browsing completed and upcoming matches
- a draw tab with a proper bracket view
- edition history so you can jump between different years

The bracket view is the part I have wanted for a while. It makes it much easier to see where a player sits in the draw, who they might meet next, and what path they need to take to go deep in the event.

## Navigation around the site is better now

I also tightened up the linking between pages.

Player names and tournament names now connect much more cleanly across the site, so it is easier to move from a match to a player page, or from a player result to the tournament it happened in.

## The home page should feel faster

The home page has been reworked so it can load different sections separately instead of waiting for everything at once.

In plain English: it should feel quicker and less clunky, especially when the site is pulling in a lot of match data.

## Prediction work has properly started

This part is mostly behind the scenes for now, but it is an important step.

I have now started building the new prediction system properly. Teelo is starting to learn from things like:

- player strength
- recent form
- head-to-head history
- surface context
- schedule and activity patterns

That does **not** mean polished predictions are live on the website yet. They are not. But the modelling pipeline now exists, which means I can start testing what is actually good enough to put in front of users.

## Summary

This was a strong couple of days for the user-facing side of Teelo.

Search is live, tournament pages are much better, the site navigation is cleaner, and the home page should feel snappier. On top of that, the next generation of prediction tools is finally moving from idea to actual implementation.