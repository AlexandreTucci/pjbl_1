package q6_minmax_brasil_2016;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q6 - Combiner para min/max
 *
 * O Combiner roda localmente em cada nó ANTES de enviar ao Reducer,
 * reduzindo a quantidade de dados trafegados na rede.
 *
 * Tem a mesma lógica do Reducer: encontra o min e max local.
 *
 * Entrada:  ("Brasil2016", [MinMaxWritable, MinMaxWritable, ...])
 * Saída:    ("Brasil2016", MinMaxWritable) — com o min e max parciais
 */
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