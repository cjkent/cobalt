/**
 * Copyright (C) 2015 - present by OpenGamma Inc. and the OpenGamma group of companies
 * <p>
 * Please see distribution for license.
 */
package io.github.cjkent.cobalt;

import java.util.Arrays;

/**
 *
 */
public class VarCalculator {

  private final double[] sortedValues = {};

  private VarCalculator() {
  }

  public double expectedShortfallAtConfidenceLevel(double confidenceLevel, boolean roundToNearestLoss) {
    double scenario = scenarioFromConfidenceLevel(confidenceLevel);
    return expectedShortfallOfLosses(roundToNearestLoss ? Math.round(scenario) : scenario);
  }

  public double[] worstNLosses(int losses) {
    if (losses < 1 || losses > sortedValues.length) {
      throw new IllegalArgumentException("Number of losses to obtain for must be between 1 and "
          + sortedValues.length + " got: " + losses);
    }
    return Arrays.copyOfRange(sortedValues, 0, losses);
  }

  public double expectedShortfallOfLosses(double lossesToAverage) {
    if (lossesToAverage < 1 || lossesToAverage > sortedValues.length) {
      throw new IllegalArgumentException("Number of losses to obtain expected shortfall for must be between 1 and "
          + sortedValues.length + " got: " + lossesToAverage);
    }
    double lossIndex = lossesToAverage - 1; // as collection 0 indexed
    int lowerIndex = (int) Math.floor(lossIndex);
    int upperIndex = (int) Math.ceil(lossIndex);
    double losses = 0d;
    // if we have a partial scenario weight that loss by the fractional part of the number e.g.
    // for 3.75 we get: (1st loss + 2nd loss + 3rd loss + 0.75*4th loss) / 3.75
    for (int i = 0; i <= lowerIndex; i++) {
      losses += sortedValues[i];
    }
    // add in any fractional weighted loss
    if (lowerIndex != upperIndex) {
      double weight = lossIndex - lowerIndex;
      losses += weight * sortedValues[upperIndex];
    }
    return losses / lossesToAverage;
  }

  public double varAtConfidenceLevel(double confidenceLevel, boolean roundToNearestWholeScenario) {
    double scenario = scenarioFromConfidenceLevel(confidenceLevel);
    return varAtScenario(roundToNearestWholeScenario ? Math.round(scenario) : scenario);
  }

  public double varAtScenario(double scenario) {
    if (scenario < 1 || scenario > sortedValues.length) {
      throw new IllegalArgumentException("Scenario id to obtain var for must be between 1 and : " +
          sortedValues.length + " got: " + scenario);
    }
    double lossIndex = scenario - 1; // as collection 0 indexed
    // VaR Scenario = 3.75, VaR loss = 0.25 * 3rd scenario loss + 0.75 * 4th scenario loss
    int lowerIndex = (int) Math.floor(lossIndex);
    int upperIndex = (int) Math.ceil(lossIndex);
    double upperPerc = lossIndex - lowerIndex;
    double lowerPerc = 1d - upperPerc;
    return sortedValues[lowerIndex] * lowerPerc + sortedValues[upperIndex] * upperPerc;
  }

  public double scenarioFromConfidenceLevel(double confidenceLevel) {
    if (confidenceLevel <= 0d || confidenceLevel >= 1d) {
      throw new IllegalArgumentException("confidenceLevel should be between 0 and 1, got: " + confidenceLevel);
    }
    double scenarioId = sortedValues.length * (1 - confidenceLevel);
    if (scenarioId < 1) {
      throw new IllegalArgumentException("Not enough values in series (" + sortedValues.length
          + ") to obtain scenario id > 1 for confidence level " + confidenceLevel);
    }
    return scenarioId;
  }
}
