package cs455.hadoop.census.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

public class MapMultiple implements Writable {

    private double own;
    private double rent;
    private int population;
    private BigDecimal percentRent;
    private BigDecimal percentOwn;
    private double maleNeverMarried;
    private double marriageableMales;
    private BigDecimal percentMaleNeverMarried;
    private double femaleNeverMarried;
    private double marriageableFemales;
    private BigDecimal percentFemaleNeverMarried;
    private double insideUrban;
    private double outsideUrban;
    private double rural;

    public MapMultiple() {

    }

    public MapMultiple(double own, double rent, int population, double maleNeverMarried, double femaleNeverMarried) {
        this.own = own;
        this.rent = rent;
        this.population = population;
        this.maleNeverMarried = maleNeverMarried;
        this.femaleNeverMarried = femaleNeverMarried;
    }

    //readFields and write must have the same order and contain all i/o variables!
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        own = dataInput.readDouble();
        rent = dataInput.readDouble();
        maleNeverMarried = dataInput.readDouble();
        femaleNeverMarried = dataInput.readDouble();
        marriageableMales = dataInput.readDouble();
        marriageableFemales = dataInput.readDouble();
        insideUrban = dataInput.readDouble();
        outsideUrban = dataInput.readDouble();
        rural = dataInput.readDouble();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(own);
        dataOutput.writeDouble(rent);
        dataOutput.writeDouble(maleNeverMarried);
        dataOutput.writeDouble(femaleNeverMarried);
        dataOutput.writeDouble(marriageableMales);
        dataOutput.writeDouble(marriageableFemales);
        dataOutput.writeDouble(insideUrban);
        dataOutput.writeDouble(outsideUrban);
        dataOutput.writeDouble(rural);
    }

    //q1
    public double getOwn() {
        return own;
    }
    public void setOwn(double own) {
        this.own += own;
    }
    public double getRent() {
        return rent;
    }
    public void setRent(double rent) {
        this.rent += rent;
    }
    public void setPercentRent(BigDecimal percentRent) {this.percentRent = percentRent;}
    public void setPercentOwn(BigDecimal percentOwn) {this.percentOwn = percentOwn;}

    //q2
    public void setMaleNeverMarried(double maleNeverMarried) {this.maleNeverMarried = maleNeverMarried;}
    public void setFemaleNeverMarried(double femaleNeverMarried) {this.femaleNeverMarried = femaleNeverMarried;}
    public double getMaleNeverMarried() {return maleNeverMarried;}
    public double getFemaleNeverMarried() {return femaleNeverMarried;}
    public double getMarriageableMales() {return marriageableMales;}
    public void setMarriageableMales(double marriageableMales) {this.marriageableMales = marriageableMales;}
    public double getMarriageableFemales() {return marriageableFemales;}
    public void setMarriageableFemales(double marriageableFemales) {this.marriageableFemales = marriageableFemales;}

    //q4
    public double getInsideUrban() {return insideUrban;}
    public void setInsideUrban(double insideUrban) {this.insideUrban = insideUrban;}
    public double getOutsideUrban() {return outsideUrban;}
    public void setOutsideUrban(double outsideUrban) {this.outsideUrban = outsideUrban;}
    public double getRural() {return rural;}
    public void setRural(double rural) {this.rural = rural;}

    @Override
    public String toString() {
        return  rent + ":" +
                own + ":" +
                population + ":" +
                percentRent + ":" +
                percentOwn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapMultiple that = (MapMultiple) o;

        if (own != that.own) return false;
        if (rent != that.rent) return false;
        return population == that.population;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(own);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(rent);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + population;
        result = 31 * result + (percentRent != null ? percentRent.hashCode() : 0);
        result = 31 * result + (percentOwn != null ? percentOwn.hashCode() : 0);
        return result;
    }
}
