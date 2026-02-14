---
title: ELO values are now live (and rankings are next)
date: 2026-02-14
author: Cammy
excerpt: ELO values are starting to appear on Teelo, with rankings not far away. These numbers are still preliminary while optimisation work is ahead.
category: Dev
draft: false
---

# ELO values are now live (and rankings are next)

ELO values are starting to appear on the website.

That means rankings are not far away.

## Important context: these values are preliminary

The current numbers are an early version of the system, not the final tuned version.

I still need to run full optimisation passes, and there is more validation work to do before I treat these ratings as stable long-term.

So if you see fishy looking values here and there, that is expected at this stage.

## One system is not enough

A big part of this next phase is splitting rating behaviour by context:

- men and women need separate systems
- different tournament levels need different behaviour
- need to analyse the impact of advanced elo features: inactivity decay, uncertainty boost, margin-of-victory scaling

Those distinctions matter if ELO is going to be useful for both rankings and predictions.

## The goal: stable historical ELO

Where I want to get to is a point where historical ELO values are mostly stable and only need infrequent updates.

That gives us a much cleaner foundation for everything else:

- rankings on the site
- model features
- fair performance tracking over time

## What has to happen before that

To get there properly, I need to rebuild the prediction systems from the previous version of Teelo and start analysing outcomes again.

That analysis loop is what will tell me which ELO settings are actually helping, where drift still exists, and what should be locked in.

So this release is an important step, but not the end state.

## Summary

ELO is now visible on Teelo and rankings are on the way.

The current values are preliminary, and the real work now is optimisation, system separation by context, and rebuilding prediction analysis so the historical ratings can settle into something trustworthy.
