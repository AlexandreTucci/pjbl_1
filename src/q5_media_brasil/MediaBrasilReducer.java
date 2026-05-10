package q5_media_brasil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MediaBrasilReducer extends Reducer<Text, MediaWritable, Text, MediaWritable> {

    @Override
    protected void reduce(Text key, Iterable<MediaWritable> values, Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0.0;
        long contagemTotal = 0;

        for (MediaWritable val : values) {
            somaTotal += val.getSoma();
            contagemTotal += val.getContagem();
        }

        context.write(key, new MediaWritable(somaTotal, contagemTotal));
    }
}