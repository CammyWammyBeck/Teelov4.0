---
title: Earlier Predictions and Tournament Predictions
date: 2026-02-02
author: Cammy
excerpt: Predicting tournament winners and getting match predictions out faster by understanding the draw.
category: Dev
draft: false
---

# Earlier Predictions and Tournament Predictions

A frustration with the previous version of Teelo was that predictions had to wait until a schedule was published before we knew who was playing in upcoming matches. That was often the night before the match, even though the deciding matches could have been played the day before — or even earlier.

## Understanding the Draw

Something I have wanted to implement for a while is predicting each player's chance of winning an entire tournament. To do this, I need to scrape the draw, predict every possible combination of matches from each round onwards, and then calculate the overall probability of each player winning.

For example, if Player A has a 70% chance of beating Player B in the semi-final, and a 60% chance of beating Player C but only a 40% chance of beating Player D in the final, then the probability of Player A winning the tournament depends on the combined likelihood of each path through the draw. Multiply it all out across every possible combination of results, and you get a single number for each player's chance of lifting the trophy.

## Two Birds, One Stone

This also solves the earlier predictions problem. By understanding the draw and knowing who will or might play each other in following rounds, I can predict the next round as soon as results come in — no need to wait for the official schedule.

Technically, I could post predictions for every possible future match as soon as the draw is released. But the numbers get out of hand quickly. Even a 32-player ATP 250 draw would produce a huge number of potential matchups across all rounds, and larger draws would be even worse. Instead, it makes more sense to only predict one round ahead. But it does open up possibibilites of creating a UI on the website that allows users to simulate a whole tournament.

## The Data Pipeline

To make this all work, I've had to rethink how data flows through the system. It's no longer just "fetch today's matches." It's a three-step process that keeps updating information until the match information is complete.

First up is **Draw Ingestion**. This is the big picture view. As soon as a tournament draw is out, I scrape the entire bracket. This tells me every potential matchup from the first round to the final. Matches are created in the system as "upcoming"—we know who *could* play, even if we don't know *when* yet.

Next comes **Schedule Ingestion**. This runs along side the draw ingestion and looks at the official Order of Play. It takes those "upcoming" matches and gives them a concrete time and court, information that we can't get from the draw. It can also give us a signal if the draw has been updated because of withdrawals.

Finally, **Results Ingestion**. Once the ball is hit, we need the score. This step grabs the final results, updates the winners, and—crucially—unlocks the next round of the draw. When Player A beats Player B, the system automatically propagates Player A to the next round in the bracket, creating a new "upcoming" match against whoever is waiting for them.

It's a cycle that repeats until a champion is crowned, ensuring Teelo always knows the state of play, even before the schedule is published.

## Summary

Two improvements are on the way: tournament winner predictions that give each player an overall probability of winning, and faster match predictions that don't have to wait for the schedule. Both come from the same underlying change — scraping and understanding the full draw. Expect to see these once the prediction system is up and running.