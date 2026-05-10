package q4_fluxo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FluxoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text fluxo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        if (linha.startsWith("country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 5) return;

        String flow = campos[4].trim();

        if (flow.isEmpty()) return;

        fluxo.set(flow);
        context.write(fluxo, UM);
    }
}

