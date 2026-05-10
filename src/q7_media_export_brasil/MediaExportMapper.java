package q7_media_export_brasil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MediaExportMapper extends Mapper<LongWritable, Text, Text, MediaWritable> {

    private final Text ano = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        if (linha.startsWith("Country")) return;

        String[] campos = linha.split(";");

        if (campos.length < 10) return;

        String pais     = campos[0].trim();
        String year     = campos[1].trim();
        String flow     = campos[4].trim();
        String precoStr = campos[5].trim();

        if (!pais.equalsIgnoreCase("Brazil")) return;
        if (!flow.equalsIgnoreCase("Export")) return;

        if (year.isEmpty() || precoStr.isEmpty()) return;

        try {
            double preco = Double.parseDouble(precoStr);
            ano.set(year);
            context.write(ano, new MediaWritable(preco, 1));
        } catch (NumberFormatException e) {
        }
    }
}