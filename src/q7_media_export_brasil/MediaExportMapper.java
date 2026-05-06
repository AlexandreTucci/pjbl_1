package q7_media_export_brasil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q7 - Valor médio das transações por ano, somente Export no Brasil
 *
 * Mapper: filtra Brasil + Export, emite (ano, MediaWritable(preco, 1))
 *
 * Entrada:  cada linha do CSV
 * Saída:    ("2016", MediaWritable(6088.0, 1))
 */
public class MediaExportMapper extends Mapper<LongWritable, Text, Text, MediaWritable> {

    private final Text ano = new Text();

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

        String pais     = campos[0].trim();
        String year     = campos[1].trim();
        String flow     = campos[4].trim();
        String precoStr = campos[5].trim();

        // 4. Filtra somente Brasil e Export
        if (!pais.equalsIgnoreCase("Brazil")) return;
        if (!flow.equalsIgnoreCase("Export")) return;

        // 5. Trata campos vazios
        if (year.isEmpty() || precoStr.isEmpty()) return;

        try {
            double preco = Double.parseDouble(precoStr);
            ano.set(year);
            context.write(ano, new MediaWritable(preco, 1));
        } catch (NumberFormatException e) {
            // Ignora linhas com preço inválido
        }
    }
}