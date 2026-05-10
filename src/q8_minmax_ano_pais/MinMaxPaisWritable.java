package q8_minmax_ano_pais;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxPaisWritable implements Writable {

    private double minAmount;
    private double maxAmount;
    private String minCommodity;
    private String maxCommodity;

    public MinMaxPaisWritable() {
        this.minAmount = Double.MAX_VALUE;
        this.maxAmount = -Double.MAX_VALUE;
        this.minCommodity = "";
        this.maxCommodity = "";
    }

    public MinMaxPaisWritable(double minAmount, String minCommodity,
                              double maxAmount, String maxCommodity) {
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
        this.minCommodity = minCommodity;
        this.maxCommodity = maxCommodity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(minAmount);
        out.writeDouble(maxAmount);
        out.writeUTF(minCommodity);
        out.writeUTF(maxCommodity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.minAmount = in.readDouble();
        this.maxAmount = in.readDouble();
        this.minCommodity = in.readUTF();
        this.maxCommodity = in.readUTF();
    }

    public double getMinAmount() { return minAmount; }
    public double getMaxAmount() { return maxAmount; }
    public String getMinCommodity() { return minCommodity; }
    public String getMaxCommodity() { return maxCommodity; }

    public void setMinAmount(double minAmount) { this.minAmount = minAmount; }
    public void setMaxAmount(double maxAmount) { this.maxAmount = maxAmount; }
    public void setMinCommodity(String minCommodity) { this.minCommodity = minCommodity; }
    public void setMaxCommodity(String maxCommodity) { this.maxCommodity = maxCommodity; }

    @Override
    public String toString() {
        return String.format(
                "\nMIN: %.2f (%s)\nMAX: %.2f (%s)",
                minAmount, minCommodity,
                maxAmount, maxCommodity
        );
    }
}