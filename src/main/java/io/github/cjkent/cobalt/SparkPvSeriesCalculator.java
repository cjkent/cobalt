package io.github.cjkent.cobalt;

import java.util.concurrent.ExecutorService;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.util.concurrent.MoreExecutors;
import com.opengamma.strata.basics.currency.CurrencyAmount;
import com.opengamma.strata.collect.Messages;
import com.opengamma.strata.collect.result.Result;
import com.opengamma.strata.engine.calculations.CalculationResult;
import com.opengamma.strata.engine.calculations.CalculationTask;
import com.opengamma.strata.engine.calculations.function.result.ScenarioResult;
import com.opengamma.strata.engine.config.CalculationTaskConfig;
import com.opengamma.strata.engine.config.CalculationTasksConfig;
import com.opengamma.strata.engine.marketdata.ScenarioCalculationEnvironment;

/**
 *
 */
public class SparkPvSeriesCalculator extends PvSeriesCalculator {

    private final int nSlices;
    private final String master;

    protected SparkPvSeriesCalculator(int nTrades, int nSlices, ExecutorService executor, String master) {
        super(nTrades, executor);
        this.nSlices = nSlices;
        this.master = master;
    }

    public static void main(String[] args) {
        int nTrades = Integer.parseInt(args[0]);
        String master = args[1];
        int nSlices = Integer.parseInt(args[2]);
        System.out.println(Messages.format("Calculating PV for {} trades, {} scenarios", nTrades, N_SCENARIOS));
        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        Result<double[]> result = new SparkPvSeriesCalculator(nTrades, nSlices, executor, master).calculate();

        if (result.isSuccess()) {
            System.out.println("Results size = " + result.getValue().length);
        } else {
            System.out.println("Calculation failed: " + result.getFailure().getMessage());
        }
    }

    @Override
    protected Result<double[]> calculate(CalculationTasksConfig tasksConfig, ScenarioCalculationEnvironment env) {
        //SparkConf conf = new SparkConf().setAppName("Cobalt").setMaster("local[2]");
        SparkConf conf = new SparkConf().setAppName("Cobalt").setMaster("spark://" + master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        @SuppressWarnings("unchecked")
        double[] presentValues = sc.parallelize(tasksConfig.getTaskConfigurations(), nSlices)
                .map(SparkPvSeriesCalculator::createTask)
                .map(task -> task.execute(env))
                .map(CalculationResult::getResult)
                .map(result -> (ScenarioResult<CurrencyAmount>) result.getValue())
                .map(PvSeriesCalculator::toArray)
                .reduce(PvSeriesCalculator::add);

        return Result.success(presentValues);
    }

    /**
     * Creates a task for performing a single calculation.
     *
     * @param config  configuration for the task
     * @return a task for performing a single calculation
     */
    private static CalculationTask createTask(CalculationTaskConfig config) {
        return new CalculationTask(
                config.getTarget(),
                config.getRowIndex(),
                config.getColumnIndex(),
                config.createFunction(),
                config.getMarketDataMappings(),
                config.getReportingRules());
    }
}
