---
title: Weekly Update — Teelo Now Shows Why It Likes a Matchup
date: 2026-04-26
author: Cam
excerpt: This week Teelo got a lot better at explaining its predictions, match pages became easier to use, and a few annoying live-tournament pipeline issues got cleaned up before they could cause more chaos.
category: Dev
draft: false
---

# Weekly Update — Teelo Now Shows Why It Likes a Matchup

This week was a nice mix of product improvements people can actually see and some cleanup work that should make live tournaments a lot less fragile behind the scenes.

The biggest user-facing change is that match pages can now show more of *why* Teelo leans one way in a matchup, rather than just spitting out a percentage and leaving it there.

## Match pages now give more context behind the prediction

One thing I have wanted for a while is a better way to explain what is driving a prediction in plain English.

That is now in place.

Teelo is storing per-feature prediction explanations, which means match detail pages can surface the key factors behind a number instead of treating the model like a black box. From a user point of view, the important part is simple: if Teelo likes one player, you can now get a clearer sense of *why*.

I think that makes the site more useful. A raw percentage is fine, but the real value is understanding what is pushing the matchup in that direction.

## Match rows should feel nicer to use

There was also some tidy-up work on how match rows behave across the site.

The full row is now clickable more consistently, and I cleaned up some of the rendering logic so those lists are handled in one place instead of being stitched together in slightly different ways depending on the page.

It is not a headline feature, but it is the sort of polish that makes Teelo feel less awkward when you are bouncing between matches, players, and tournaments.

## Live tournament updates got a lot more defensive

A big chunk of the week went into fixing some annoying edge cases in the current-results pipeline.

The short version is that I found a couple of ways live tournament updates could get themselves into a bad state — especially when results ingestion only partially succeeded, or when later stages rolled back work that should already have been kept.

That matters because it can leave a draw looking stale even though new results are available.

Those transaction and checkpointing issues have now been tightened up, and I also used the Madrid WTA data as a real-world test case to repair some missing results and make sure the fix held up properly.

This is very much backend reliability work, but it is important. If tournament state is off, the rest of the site gets shaky pretty quickly.

## Summary

So this week was really about making Teelo easier to trust.

The prediction pages are starting to explain themselves better, match navigation is smoother, and the live tournament pipeline should be a lot less likely to freeze or quietly skip important updates.

Still plenty to do, but this felt like a good week for moving Teelo from “the number says this” toward “here is the number, and here is the reasoning behind it.”