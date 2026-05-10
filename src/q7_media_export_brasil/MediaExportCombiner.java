package q7_media_export_brasil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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