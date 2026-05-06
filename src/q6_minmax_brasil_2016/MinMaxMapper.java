package q6_minmax_brasil_2016;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q6 - Transação mais cara e mais barata no Brasil em 2016
 *
 * Mapper: filtra Brasil + ano 2016, emite ("Brasil2016", MinMaxWritable)
 * onde min e max são inicialmente o mesmo valor (o preço da linha atual).
 *
 * Entrada:  cada linha do CSV
 * Saída:    ("Brasil2016", MinMaxWritable(preco, commodity, preco, commodity))
 */
public class MinMaxMapper extends Mapper<LongWritable, Text, Text, MinMaxWritable> {

    private static final Text CHAVE = new Text("Brasil2016");

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
        String precoStr  = campos[5].trim();

        // 4. Filtra somente Brasil em 2016
        if (!pais.equalsIgnoreCase("Brazil")) return;
        if (!year.equals("2016")) return;

        // 5. Trata campos vazios
        if (precoStr.isEmpty() || commodity.isEmpty()) return;

        try {
            double preco = Double.parseDouble(precoStr);
            // Emite com min e max sendo o mesmo valor inicial
            context.write(CHAVE, new MinMaxWritable(preco, commodity, preco, commodity));
        } catch (NumberFormatException e) {
            // Ignora linhas com preço inválido
        }
    }
}