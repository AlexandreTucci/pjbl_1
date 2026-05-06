package q8_minmax_ano_pais;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Q8 - Comparable Writable usado como CHAVE do MapReduce
 *
 * Armazena:
 * - ano: ano da transação
 * - pais: país da transação
 *
 * Implementa WritableComparable para poder ser usado como chave,
 * permitindo que o Hadoop ordene e agrupe por (ano, país).
 */
public class AnoPaisWritable implements WritableComparable<AnoPaisWritable> {

    private String ano;
    private String pais;

    // Construtor padrão obrigatório para deserialização
    public AnoPaisWritable() {
        this.ano = "";
        this.pais = "";
    }

    public AnoPaisWritable(String ano, String pais) {
        this.ano = ano;
        this.pais = pais;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ano);
        out.writeUTF(pais);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ano = in.readUTF();
        this.pais = in.readUTF();
    }

    /**
     * Comparação usada pelo Hadoop para ordenar e agrupar chaves.
     * Ordena primeiro por ano, depois por país.
     */
    @Override
    public int compareTo(AnoPaisWritable other) {
        int cmpAno = this.ano.compareTo(other.ano);
        if (cmpAno != 0) return cmpAno;
        return this.pais.compareTo(other.pais);
    }

    public String getAno() { return ano; }
    public String getPais() { return pais; }

    @Override
    public String toString() {
        return ano + "\t" + pais;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnoPaisWritable)) return false;
        AnoPaisWritable other = (AnoPaisWritable) o;
        return ano.equals(other.ano) && pais.equals(other.pais);
    }

    @Override
    public int hashCode() {
        return 31 * ano.hashCode() + pais.hashCode();
    }
}