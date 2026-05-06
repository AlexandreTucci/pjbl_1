package q8_minmax_ano_pais;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q8 - Transação com maior e menor preço (amount) por ano e país
 *
 * Mapper: emite (AnoPaisWritable(ano, pais), MinMaxPaisWritable(amount, commodity))
 *
 * Entrada:  cada linha do CSV
 * Saída:    (AnoPaisWritable("2016", "Brazil"), MinMaxPaisWritable(...))
 */
public class MinMaxPaisMapper extends Mapper<LongWritable, Text, AnoPaisWritable, MinMaxPaisWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        // 1. Remove o cabeçalho
        if (linha.startsWith("Country")) return;

        // 2. Divide pelo separador ";"
        String[] campos = linha.split(";");

        // 3. Trata dados faltantes
        if (campos.length < 10) return;

        String pais      = campos[0].trim();
        String year      = campos[1].trim();
        String commodity = campos[3].trim();
        String amountStr = campos[8].trim(); // coluna Amount (índice 8)

        // 4. Trata campos vazios
        if (pais.isEmpty() || year.isEmpty() || amountStr.isEmpty() || commodity.isEmpty()) return;

        try {
            double amount = Double.parseDouble(amountStr);
            AnoPaisWritable chave = new AnoPaisWritable(year, pais);

            // Min e max inicialmente são o mesmo valor
            MinMaxPaisWritable valor = new MinMaxPaisWritable(amount, commodity, amount, commodity);

            context.write(chave, valor);
        } catch (NumberFormatException e) {
            // Ignora linhas com amount inválido
        }
    }
}