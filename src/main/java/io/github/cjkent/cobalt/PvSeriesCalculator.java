/**
 * Copyright (C) 2015 - present by OpenGamma Inc. and the OpenGamma group of companies
 * <p>
 * Please see distribution for license.
 */
package io.github.cjkent.cobalt;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.opengamma.strata.basics.currency.CurrencyAmount;
import com.opengamma.strata.collect.result.Result;
import com.opengamma.strata.engine.Column;
import com.opengamma.strata.engine.calculations.CalculationTasks;
import com.opengamma.strata.engine.calculations.DefaultCalculationRunner;
import com.opengamma.strata.engine.calculations.function.result.ScenarioResult;
import com.opengamma.strata.engine.config.CalculationTasksConfig;
import com.opengamma.strata.engine.config.Measure;
import com.opengamma.strata.engine.config.ReportingRules;
import com.opengamma.strata.engine.marketdata.CalculationRequirements;
import com.opengamma.strata.engine.marketdata.DefaultMarketDataFactory;
import com.opengamma.strata.engine.marketdata.MarketEnvironment;
import com.opengamma.strata.engine.marketdata.ScenarioCalculationEnvironment;
import com.opengamma.strata.engine.marketdata.config.MarketDataConfig;
import com.opengamma.strata.engine.marketdata.functions.ObservableMarketDataFunction;
import com.opengamma.strata.engine.marketdata.functions.TimeSeriesProvider;
import com.opengamma.strata.engine.marketdata.mapping.FeedIdMapping;
import com.opengamma.strata.engine.marketdata.scenarios.Perturbation;
import com.opengamma.strata.engine.marketdata.scenarios.PerturbationMapping;
import com.opengamma.strata.engine.marketdata.scenarios.ScenarioDefinition;
import com.opengamma.strata.examples.marketdata.ExampleMarketData;
import com.opengamma.strata.examples.marketdata.MarketDataBuilder;
import com.opengamma.strata.finance.Trade;
import com.opengamma.strata.function.OpenGammaPricingRules;
import com.opengamma.strata.function.marketdata.curve.DiscountFactorsMarketDataFunction;
import com.opengamma.strata.function.marketdata.scenarios.curves.AnyCurveFilter;
import com.opengamma.strata.function.marketdata.scenarios.curves.CurveParallelShift;
import com.opengamma.strata.market.curve.Curve;

public abstract class PvSeriesCalculator {

    public static final int N_SCENARIOS = 1250;

    protected final DefaultCalculationRunner calculationRunner;

    private static final List<Column> COLUMNS = ImmutableList.of(Column.of(Measure.PRESENT_VALUE));
    private static final LocalDate VALUATION_DATE = LocalDate.of(2014, 1, 22);

    private static final DefaultMarketDataFactory MARKET_DATA_FACTORY = new DefaultMarketDataFactory(
            TimeSeriesProvider.none(),
            ObservableMarketDataFunction.none(),
            FeedIdMapping.identity(),
            new DiscountFactorsMarketDataFunction());

    private static final MarketDataBuilder MARKET_DATA_BUILDER = ExampleMarketData.builder();

    private final int nTrades;

    protected PvSeriesCalculator(int nTrades, ExecutorService executor) {
        this.nTrades = nTrades;
        calculationRunner = new DefaultCalculationRunner(executor);
    }

    public Result<double[]> calculate() {
        MarketEnvironment snapshot = MARKET_DATA_BUILDER.buildSnapshot(VALUATION_DATE);
        CalculationTasksConfig tasksConfig = calculationRunner.createCalculationConfig(
                trades(nTrades),
                COLUMNS,
                OpenGammaPricingRules.standard(),
                MARKET_DATA_BUILDER.rules(),
                ReportingRules.empty());

        CalculationTasks calculationTasks = calculationRunner.createCalculationTasks(tasksConfig);
        CalculationRequirements requirements = calculationTasks.getRequirements();
        ScenarioCalculationEnvironment env = MARKET_DATA_FACTORY.buildScenarioCalculationEnvironment(
                requirements,
                snapshot,
                scenarioDefinition(),
                MarketDataConfig.empty());

        return Stopwatch.time("Calculations took", () -> calculate(tasksConfig, env));
    }

    private static ScenarioDefinition scenarioDefinition() {
        ImmutableList.Builder<Perturbation<Curve>> perturbations = ImmutableList.builder();
        // Base scenario
        perturbations.add(CurveParallelShift.absolute(0));
        Random random = new Random();

        for (int i = 1; i < N_SCENARIOS; i++) {
            // between -10bp (incl) and 10bp (excl)
            double shiftAmount = ((random.nextDouble() * 20) - 10) / 10_000;
            perturbations.add(CurveParallelShift.absolute(shiftAmount));
        }

        PerturbationMapping<Curve> mapping =
                PerturbationMapping.of(Curve.class, AnyCurveFilter.INSTANCE, perturbations.build());
        return ScenarioDefinition.ofMappings(mapping);
    }

    private static List<Trade> trades(int nTrades) {
        List<Trade> trades = TradeFactory.createSwapTrades();
        List<Trade> allTrades = Lists.newArrayListWithExpectedSize(nTrades);
        int loopCount = nTrades / trades.size();

        for (int i = 0; i < loopCount; i++) {
            allTrades.addAll(trades);
        }
        allTrades.addAll(trades.subList(0, nTrades % trades.size()));
        return allTrades;
    }

    static double[] add(double[] array1, double[] array2) {
        int len1 = array1.length;
        int len2 = array2.length;

        if (len1 != len2) {
            throw new IllegalArgumentException("Arrays cannot be added as they differ in length");
        }
        for (int i = 0; i < len1; i++) {
            array1[i] += array2[i];
        }
        return array1;
    }

    protected abstract Result<double[]> calculate(
            CalculationTasksConfig tasksConfig,
            ScenarioCalculationEnvironment env);

    protected static double[] toArray(ScenarioResult<CurrencyAmount> scenarioResult) {
        return scenarioResult.stream().map(CurrencyAmount::getAmount).mapToDouble(v -> v).toArray();
    }
}
