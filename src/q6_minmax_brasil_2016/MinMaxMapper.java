package q6_minmax_brasil_2016;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxMapper extends Mapper<LongWritable, Text, Text, MinMaxWritable> {

    private static final Text CHAVE = new Text("Brasil2016");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        if (linha.startsWith("Country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 10) return;

        String pais      = campos[0].trim();
        String year      = campos[1].trim();
        String commodity = campos[3].trim();
        String precoStr  = campos[5].trim();

        if (!pais.equalsIgnoreCase("Brazil")) return;
        if (!year.equals("2016")) return;

        if (precoStr.isEmpty() || commodity.isEmpty()) return;

        try {
            double preco = Double.parseDouble(precoStr);
            context.write(CHAVE, new MinMaxWritable(preco, commodity, preco, commodity));
        } catch (NumberFormatException e) {
        }
    }
}