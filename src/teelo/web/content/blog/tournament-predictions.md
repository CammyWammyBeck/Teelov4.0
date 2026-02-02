---
title: Earlier Predictions and Tournament Predictions
date: 2026-02-02
author: Cammy
excerpt: Predicting tournament winners and getting match predictions out faster by understanding the draw.
category: Dev
draft: true
---

# Earlier Predictions and Tournament Predictions

A frustration with the previous version of Teelo was that predictions had to wait until a schedule was published before we knew who was playing in upcoming matches. That was often the night before the match, even though the deciding matches could have been played the day before — or even earlier. By the time predictions went out, they weren't all that useful.

## Understanding the Draw

Something I have wanted to implement for a while is predicting each player's chance of winning an entire tournament. To do this, I need to scrape the draw, predict every possible combination of matches from each round onwards, and then calculate the overall probability of each player winning.

For example, if Player A has a 70% chance of beating Player B in the semi-final, and a 60% chance of beating Player C but only a 40% chance of beating Player D in the final, then the probability of Player A winning the tournament depends on the combined likelihood of each path through the draw. Multiply it all out across every possible combination of results, and you get a single number for each player's chance of lifting the trophy.

## Two Birds, One Stone

This also solves the earlier predictions problem. By understanding the draw and knowing who will or might play each other in following rounds, I can predict the next round as soon as results come in — no need to wait for the official schedule.

Technically, I could post predictions for every possible future match as soon as the draw is released. But the numbers get out of hand quickly. Even a 32-player ATP 250 draw would produce a huge number of potential matchups across all rounds, and larger draws would be even worse. Instead, it makes more sense to predict one or two rounds ahead and update as results come in.

## Summary

Two improvements are on the way: tournament winner predictions that give each player an overall probability of winning, and faster match predictions that don't have to wait for the schedule. Both come from the same underlying change — scraping and understanding the full draw. Expect to see these once the prediction system is up and running.
