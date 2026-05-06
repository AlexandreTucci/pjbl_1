package q8_minmax_ano_pais;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q8 - Transação com maior e menor preço (amount) por ano e país
 *
 * Reducer: recebe os min/max parciais do Combiner e encontra o global
 * para cada combinação (ano, país).
 *
 * Entrada:  (AnoPaisWritable("2016","Brazil"), [MinMaxPaisWritable parcial, ...])
 * Saída:    (AnoPaisWritable("2016","Brazil"), MinMaxPaisWritable global)
 */
public class MinMaxPaisReducer extends Reducer<AnoPaisWritable, MinMaxPaisWritable, AnoPaisWritable, MinMaxPaisWritable> {

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