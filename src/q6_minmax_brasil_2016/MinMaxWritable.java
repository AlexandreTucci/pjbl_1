package q6_minmax_brasil_2016;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Q6 - Writable customizado para armazenar min e max
 *
 * Armazena:
 * - minPreco: menor preço encontrado
 * - maxPreco: maior preço encontrado
 * - minCommodity: descrição da transação mais barata
 * - maxCommodity: descrição da transação mais cara
 */
public class MinMaxWritable implements Writable {

    private double minPreco;
    private double maxPreco;
    private String minCommodity;
    private String maxCommodity;

    // Construtor padrão obrigatório para deserialização
    public MinMaxWritable() {
        this.minPreco = Double.MAX_VALUE;
        this.maxPreco = Double.MIN_VALUE;
        this.minCommodity = "";
        this.maxCommodity = "";
    }

    public MinMaxWritable(double minPreco, String minCommodity,
                          double maxPreco, String maxCommodity) {
        this.minPreco = minPreco;
        this.maxPreco = maxPreco;
        this.minCommodity = minCommodity;
        this.maxCommodity = maxCommodity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(minPreco);
        out.writeDouble(maxPreco);
        out.writeUTF(minCommodity);
        out.writeUTF(maxCommodity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.minPreco = in.readDouble();
        this.maxPreco = in.readDouble();
        this.minCommodity = in.readUTF();
        this.maxCommodity = in.readUTF();
    }

    public double getMinPreco() { return minPreco; }
    public double getMaxPreco() { return maxPreco; }
    public String getMinCommodity() { return minCommodity; }
    public String getMaxCommodity() { return maxCommodity; }

    public void setMinPreco(double minPreco) { this.minPreco = minPreco; }
    public void setMaxPreco(double maxPreco) { this.maxPreco = maxPreco; }
    public void setMinCommodity(String minCommodity) { this.minCommodity = minCommodity; }
    public void setMaxCommodity(String maxCommodity) { this.maxCommodity = maxCommodity; }

    @Override
    public String toString() {
        return String.format(
                "\nMIN: %.2f (%s)\nMAX: %.2f (%s)",
                minPreco, minCommodity,
                maxPreco, maxCommodity
        );
    }
}