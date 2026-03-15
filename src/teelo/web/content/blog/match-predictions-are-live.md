---
title: Match Predictions Are Live
date: 2026-03-15
author: Cammy
excerpt: Teelo now shows win probability predictions for upcoming matches, with a full breakdown of the factors behind each prediction.
category: Dev
draft: false
---

# Match Predictions Are Live

This is the one I have been building towards for a while. Teelo now shows match predictions on the website.

## Predictions in match tables

If you look at any match table on the site — the home page, the matches page, a tournament page — you will now see a prediction column on upcoming matches. It shows the win probability for each player as a simple percentage.

You can scan through a day of matches and immediately see which ones Teelo thinks are close and which ones look one-sided.

## The match detail page

Click on any match row and you get a dedicated match page. At the top is the prediction hero: both player names, their win percentages, and a visual bar showing who is favoured.

Below that is the prediction breakdown. This is where it gets interesting.

The breakdown shows you the actual features the model is using to make its prediction, grouped into categories like ELO ratings, recent form, head-to-head history, surface performance, and more. For each feature, you can see both players' values side by side, colour-coded to show who has the advantage.

This is not a black box. You can see exactly why Teelo thinks one player is more likely to win, and you can decide for yourself whether you agree.

## What the model looks at

The prediction model behind this is what I have been calling "baseline v2." It is a significant step up from the first version, with new feature groups covering:

- **Opponent quality** — how strong are the players that each player has been beating or losing to recently
- **Dominance** — are they winning comfortably or scraping through tight matches
- **Fatigue and scheduling** — how much tennis have they played recently, and how much rest have they had
- **Tournament history** — how have they performed at this specific event in the past
- **Confidence** — model-level signals about how reliable the prediction is likely to be

In total, the model uses around 85 features across these groups plus the existing ones like ELO, form, surface ratings, and head-to-head records.

## What comes next

This is the first version of predictions on the site. The model will keep improving as I test new features and collect more data on what actually predicts match outcomes well.

There is also more I want to do with the presentation. Prediction accuracy tracking, feature contributions and tournament-level predictions are all on the list. But getting individual match predictions live and visible was the priority, and that is now done.
