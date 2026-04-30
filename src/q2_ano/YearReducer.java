package q2_ano;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q2 - Número de transações por ano
 *
 * Reducer: soma todos os "1" para cada ano.
 *
 * Entrada:  ("2016", [1, 1, 1, ...])
 * Saída:    ("2016", total)
 */
public class YearReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

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