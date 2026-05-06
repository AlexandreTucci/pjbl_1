package q7_media_export_brasil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Q7 - Combiner para média de Export no Brasil
 *
 * O Combiner agrega parcialmente soma e contagem localmente,
 * reduzindo dados enviados ao Reducer.
 *
 * IMPORTANTE: o Combiner NÃO divide soma/contagem aqui —
 * apenas acumula, para que o Reducer calcule a média correta no final.
 *
 * Entrada:  ("2016", [MediaWritable(6088, 1), MediaWritable(3958, 1), ...])
 * Saída:    ("2016", MediaWritable(somaLocal, contagemLocal))
 */
public class MediaExportCombiner extends Reducer<Text, MediaWritable, Text, MediaWritable> {

    @Override
    protected void reduce(Text key, Iterable<MediaWritable> values, Context context)
            throws IOException, InterruptedException {

        double somaLocal = 0.0;
        long contagemLocal = 0;

        for (MediaWritable val : values) {
            somaLocal += val.getSoma();
            contagemLocal += val.getContagem();
        }

        context.write(key, new MediaWritable(somaLocal, contagemLocal));
    }
}