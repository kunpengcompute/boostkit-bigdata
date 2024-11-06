#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

agg_functions = [
    'exponentialtimedecayedcount(', 'exponentialtimedecayedmax(', 'laginframe(', 'dense_rank(', 'rank(',
    'exponentialmovingaverage(', 'sparkbar(', 'singlevalueornull(', 'meanztest(', 'studentttest(',
    'rankcorr(', 'aggthrow(', 'categoricalinformationvalue(', 'grouparraymovingavg(',
    'grouparraymovingsum(', 'simplelinearregression(', 'entropy(', 'histogram(', 'mannwhitneyutest(',
    'maxintersections(', 'groupbitmapxor(', 'groupbitmap(', 'contingency(', 'maxmappedarrays(', 'theilsu(',
    'skewpop(', 'groupbitxor(', 'groupbitor(', 'groupbitmapand(', 'exponentialtimedecayedsum(',
    'topkweighted(', 'stochasticlinearregression(', 'corr(', 'uniqcombined64(', 'cramersvbiascorrected(',
    'intervallengthsum(', 'nothing(', 'uniqcombined(', 'quantileexactweighted(',
    'summapfilteredwithoverflow(', 'summapfiltered(', 'minmappedarrays(', 'summappedarrays(', 'quantiles(',
    'sum(', 'covarpop(', 'row_number(', 'kurtpop(', 'kurtsamp(', 'cramersv(', 'skewsamp(', 'uniqexact(',
    'summapwithoverflow(', 'stddevsamp(', 'varpop(', 'corrstable(', 'quantiletimingweighted(',
    'covarpopstable(', 'stddevsampstable(', 'varsamp(', 'topk(', 'last_value(', 'quantilesexact(',
    'uniqhll12(', 'quantilebfloat16(', 'uniq(', 'min(', 'sequencenextnode(', 'quantilestimingweighted(',
    'boundingratio(', 'any(', 'anylast(', 'deltasum(', 'retention(', 'sequencematch(', 'uniqupto(',
    'windowfunnel(', 'deltasumtimestamp(', 'varsampstable(', 'uniqtheta(', 'quantilesexactweighted(',
    'max(', 'quantilesbfloat16weighted(', 'quantilebfloat16weighted(', 'sumkahan(',
    'quantilestdigestweighted(', 'groupbitand(', 'quantiletdigest(', 'quantiletdigestweighted(', 'argmax(',
    'quantiledeterministic(', 'quantilestdigest(', 'stochasticlogisticregression(', 'argmin(', 'avg(',
    'covarsampstable(', 'quantilestiming(', 'welchttest(', 'covarsamp(', 'varpopstable(',
    'quantiletiming(', 'quantileexactinclusive(', 'quantileexacthigh(', 'grouparraysample(',
    'quantilesexactlow(', 'groupbitmapor(', 'first_value(', 'exponentialtimedecayedavg(',
    'quantileexactexclusive(', 'quantileexact(', 'sumcount(', 'grouparrayinsertat(', 'quantilesexacthigh(',
    'sumwithoverflow(', 'sequencecount(', 'quantilesdeterministic(', 'groupuniqarray(', 'grouparray(',
    'anyheavy(', 'stddevpop(', 'quantile(', 'leadinframe(', 'quantilesexactexclusive(', 'count(',
    'maxintersectionsposition(', 'quantilesbfloat16(', 'quantilesexactinclusive(', 'stddevpopstable(',
    'quantileexactlow(', 'avgweighted(', 'bit_and(', 'groupbitand(', 'var_samp(', 'varsamp(',
    'covar_samp(', 'covarsamp(', 'var_pop(', 'varpop(', 'mediantdigest(', 'quantiletdigest(',
    'medianbfloat16(', 'quantilebfloat16(', 'mediantimingweighted(', 'quantiletimingweighted(',
    'mediantiming(', 'quantiletiming(', 'medianexacthigh(', 'quantileexacthigh(', 'bit_or(', 'groupbitor(',
    'mediandeterministic(', 'quantiledeterministic(', 'stddev_pop(', 'stddevpop(', 'stddev_samp(',
    'stddevsamp(', 'medianexactlow(', 'quantileexactlow(', 'mediantdigestweighted(',
    'quantiletdigestweighted(', 'medianexact(', 'quantileexact(', 'covar_pop(', 'covarpop(',
    'medianbfloat16weighted(', 'quantilebfloat16weighted(', 'medianexactweighted(',
    'quantileexactweighted(', 'bit_xor(', 'groupbitxor(', 'median(', 'quantile(',
    'sumif(', 'stddev(', 'stdev('
]