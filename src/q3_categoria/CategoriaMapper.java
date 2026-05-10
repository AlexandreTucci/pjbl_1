package q3_categoria;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CategoriaMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text categoria = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();

        if (linha.startsWith("Country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 10) return;

        String cat = campos[9].trim();

        if (cat.isEmpty()) return;

        categoria.set(cat);
        context.write(categoria, UM);
    }
}
