package q1_brasil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class BrasilMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private static final Text BRASIL = new Text("Brazil");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        if (linha.startsWith("Country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 10) return;

        String pais = campos[0].trim();

        if (pais.equalsIgnoreCase("Brazil")) {
            context.write(BRASIL, UM);
        }
    }
}