/**
 * Copyright (C) 2015 - present by OpenGamma Inc. and the OpenGamma group of companies
 * <p>
 * Please see distribution for license.
 */
package io.github.cjkent.cobalt;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.opengamma.strata.basics.currency.CurrencyAmount;
import com.opengamma.strata.collect.Messages;
import com.opengamma.strata.collect.result.Result;
import com.opengamma.strata.engine.calculations.CalculationTasks;
import com.opengamma.strata.engine.calculations.function.result.ScenarioResult;
import com.opengamma.strata.engine.config.CalculationTasksConfig;
import com.opengamma.strata.engine.marketdata.ScenarioCalculationEnvironment;

/**
 *
 */
public class LocalPvSeriesCalculator extends PvSeriesCalculator {

    protected LocalPvSeriesCalculator(int nTrades, ExecutorService executor) {
        super(nTrades, executor);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int nTrades = Integer.parseInt(args[0]);
        int nThreads = Integer.parseInt(args[1]);
        System.out.println(Messages.format(
                "Calculating PV for {} trades, {} scenarios using {} threads",
                nTrades,
                N_SCENARIOS,
                nThreads));

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        Result<double[]> result = new LocalPvSeriesCalculator(nTrades, executor).calculate();

        if (result.isSuccess()) {
            System.out.println("Results size = " + result.getValue().length);
        } else {
            System.out.println("Calculation failed: " + result.getFailure().getMessage());
        }
        executor.shutdown();
    }

    @Override
    protected Result<double[]> calculate(
            CalculationTasksConfig tasksConfig, ScenarioCalculationEnvironment env) {
        CalculationTasks tasks = calculationRunner.createCalculationTasks(tasksConfig);

        @SuppressWarnings("unchecked")
        Optional<double[]> presentValues = calculationRunner.calculate(tasks, env).getItems().parallelStream()
                .map(result -> (ScenarioResult<CurrencyAmount>) result.getValue())
                .map(PvSeriesCalculator::toArray)
                .reduce(PvSeriesCalculator::add);

        return Result.success(presentValues.get());
    }
}
