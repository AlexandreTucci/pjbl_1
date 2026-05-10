package q6_minmax_brasil_2016;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxCombiner extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

    @Override
    protected void reduce(Text key, Iterable<MinMaxWritable> values, Context context)
            throws IOException, InterruptedException {

        double minPreco = Double.MAX_VALUE;
        double maxPreco = -Double.MAX_VALUE;
        String minCommodity = "";
        String maxCommodity = "";

        for (MinMaxWritable val : values) {
            if (val.getMinPreco() < minPreco) {
                minPreco = val.getMinPreco();
                minCommodity = val.getMinCommodity();
            }
            if (val.getMaxPreco() > maxPreco) {
                maxPreco = val.getMaxPreco();
                maxCommodity = val.getMaxCommodity();
            }
        }

        context.write(key, new MinMaxWritable(minPreco, minCommodity, maxPreco, maxCommodity));
    }
}