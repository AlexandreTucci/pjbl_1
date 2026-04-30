package q4_fluxo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q4 - Número de transações por tipo de fluxo
 *
 * Reducer: soma todos os "1" para cada tipo de fluxo (Export, Import, etc.)
 *
 * Entrada:  ("Export", [1, 1, 1, ...])
 * Saída:    ("Export", total)
 */
public class FluxoReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long total = 0;

        for (LongWritable valor : values) {
            total += valor.get();
        }

        context.write(key, new LongWritable(total));
    }
}

