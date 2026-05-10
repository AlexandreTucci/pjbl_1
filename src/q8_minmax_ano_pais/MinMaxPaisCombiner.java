package q8_minmax_ano_pais;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxPaisCombiner extends Reducer<AnoPaisWritable, MinMaxPaisWritable, AnoPaisWritable, MinMaxPaisWritable> {

    @Override
    protected void reduce(AnoPaisWritable key, Iterable<MinMaxPaisWritable> values, Context context)
            throws IOException, InterruptedException {

        double minAmount = Double.MAX_VALUE;
        double maxAmount = -Double.MAX_VALUE;
        String minCommodity = "";
        String maxCommodity = "";

        for (MinMaxPaisWritable val : values) {
            if (val.getMinAmount() < minAmount) {
                minAmount = val.getMinAmount();
                minCommodity = val.getMinCommodity();
            }
            if (val.getMaxAmount() > maxAmount) {
                maxAmount = val.getMaxAmount();
                maxCommodity = val.getMaxCommodity();
            }
        }

        context.write(key, new MinMaxPaisWritable(minAmount, minCommodity, maxAmount, maxCommodity));
    }
}