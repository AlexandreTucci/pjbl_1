package q5_media_brasil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q5 - Valor médio das transações por ano somente no Brasil
 *
 * Reducer: acumula soma e contagem de todos os AvgWritable recebidos,
 * e emite (ano, AvgWritable) — o toString() calcula a média.
 *
 * Entrada:  ("2016", [AvgWritable(6088, 1), AvgWritable(3958, 1), ...])
 * Saída:    ("2016", AvgWritable(somaTotal, contagemTotal))
 *           → imprime: "2016    4500.25"  (média formatada)
 */
public class MediaBrasilReducer extends Reducer<Text, MediaWritable, Text, MediaWritable> {

    @Override
    protected void reduce(Text key, Iterable<MediaWritable> values, Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0.0;
        long contagemTotal = 0;

        // Acumula soma e contagem de todos os registros do mesmo ano
        for (MediaWritable val : values) {
            somaTotal += val.getSoma();
            contagemTotal += val.getContagem();
        }

        context.write(key, new MediaWritable(somaTotal, contagemTotal));
    }
}