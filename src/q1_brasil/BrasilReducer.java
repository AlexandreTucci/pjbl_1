package q1_brasil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q1 - Número de transações envolvendo o Brasil
 *
 * Reducer: soma todos os "1" emitidos pelo Mapper para a chave "Brazil".
 *
 * Entrada:  ("Brazil", [1, 1, 1, ...])
 * Saída:    ("Brazil", total)
 */
public class BrasilReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long total = 0;

        // Soma todos os valores recebidos
        for (LongWritable valor : values) {
            total += valor.get();
        }

        context.write(key, new LongWritable(total));
    }
}