---
title: Weekly Update — Match Scoreboard Redesign, Betting Odds, and a Smarter Prediction Model
date: 2026-03-29
author: Cammy
excerpt: Match rows got a full visual overhaul with a mini scoreboard and cleaner layout, betting odds and EV calculation landed on the matches page, and the prediction model has been updated with a new set of features under the hood.
category: Dev
draft: false
---

# Weekly Update — Match Scoreboard Redesign, Betting Odds, and a Smarter Prediction Model

This week had a mix of things users will see straight away and some that are laying groundwork for later. The match scoreboard redesign is the most visible change, but the prediction model update is probably the most significant in the long run.

## Match rows have a new look

The way completed matches display across the site has been completely reworked.

Previously, scores were a bit plain and the winner was not always obvious at a glance. Now each match row shows a proper mini scoreboard — set scores laid out cleanly, a trophy icon next to the winner's name, and a subtle background fill on the winning side to make the result immediately readable.

The prediction column has also been redesigned. Instead of a standalone bar sitting next to the scores, the probability is now baked into the row itself as a background fill, which feels a lot less cluttered.

It is a significant visual improvement across any page that shows match results.

## Betting odds and EV calculation

If you are interested in betting, there is a new feature on the matches page worth knowing about.

You can now enter the odds you are seeing from your bookmaker directly on the matches page. Teelo will calculate the expected value of that bet based on its own win probability. If the odds imply a lower probability than Teelo's model, the bet shows as positive EV.

This is an optional toggle — you can ignore it entirely if you are just here for the predictions. But for anyone who does use Teelo for betting context, it should save a lot of manual calculation.

## The prediction model has been updated

A big chunk of work this week went into improving the features that drive Teelo's predictions.

Without going into too much detail: the model now has a better understanding of how players win, not just whether they win. It picks up on things like a player's tendency to dominate sets convincingly versus grinding out close ones, how they perform in their home region, and how much their form shifts across different surfaces.

These are the kinds of patterns that a surface-level win/loss record does not capture well. The goal is predictions that hold up more consistently across different types of matches, not just ones between two well-ranked players at the biggest events.

The changes are live in the pipeline. Whether they translate to noticeably better predictions on the site is something I will keep tracking.

## ITF scraping fixed

A small but important backend fix this week: ITF match scraping had silently stopped working.

The ITF website updated their tournament calendar layout, which broke the way Teelo was loading tournament data. No ITF matches had been scraped for about a week before I caught it. The fix is in and scraping is running again.

## Summary

The match scoreboard redesign makes the site feel sharper and easier to read. Betting odds and EV calculation add a useful layer for anyone following Teelo from a betting angle. And the prediction model update is the kind of incremental improvement that should pay off gradually rather than all at once.
