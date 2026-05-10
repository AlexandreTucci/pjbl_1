package q2_ano;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text ano = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        if (linha.startsWith("Country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 10) return;

        String year = campos[1].trim();
        if (year.isEmpty()) return;

        ano.set(year);
        context.write(ano, UM);
    }
}