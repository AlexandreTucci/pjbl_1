package q6_minmax_brasil_2016;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q6 - Transação mais cara e mais barata no Brasil em 2016
 *
 * Reducer: recebe os min/max parciais do Combiner e encontra o global.
 *
 * Entrada:  ("Brasil2016", [MinMaxWritable parcial, MinMaxWritable parcial, ...])
 * Saída:    ("Brasil2016", MinMaxWritable com min e max globais)
 */
public class MinMaxReducer extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

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