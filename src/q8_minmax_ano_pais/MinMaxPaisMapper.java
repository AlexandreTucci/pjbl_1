package q8_minmax_ano_pais;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxPaisMapper extends Mapper<LongWritable, Text, AnoPaisWritable, MinMaxPaisWritable> {

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
        String amountStr = campos[8].trim(); 

        if (pais.isEmpty() || year.isEmpty() || amountStr.isEmpty() || commodity.isEmpty()) return;

        try {
            double amount = Double.parseDouble(amountStr);
            AnoPaisWritable chave = new AnoPaisWritable(year, pais);

            MinMaxPaisWritable valor = new MinMaxPaisWritable(amount, commodity, amount, commodity);

            context.write(chave, valor);
        } catch (NumberFormatException e) {
        }
    }
}