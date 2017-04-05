package cs455.hadoop.census.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

public class MapMultiple implements Writable {

    private double own;
    private double rent;
    private double population;
    private BigDecimal percentRent;
    private BigDecimal percentOwn;
    private double maleNeverMarried;
    private double femaleNeverMarried;
    private double insideUrban;
    private double outsideUrban;
    private double rural;
    private double hispanicMalesUnder18;
    private double hispanicFemalesUnder18;
    private double hispanicMales19to29;
    private double hispanicFemales19to29;
    private double hispanicMales30to39;
    private double hispanicFemales30to39;
    private double totalHispanicPopulation;
    private double totalOwnedHomes;
    private double ownedHomeValue0;
    private double ownedHomeValue1;
    private double ownedHomeValue2;
    private double ownedHomeValue3;
    private double ownedHomeValue4;
    private double ownedHomeValue5;
    private double ownedHomeValue6;
    private double ownedHomeValue7;
    private double ownedHomeValue8;
    private double ownedHomeValue9;
    private double ownedHomeValue10;
    private double ownedHomeValue11;
    private double ownedHomeValue12;
    private double ownedHomeValue13;
    private double ownedHomeValue14;
    private double ownedHomeValue15;
    private double ownedHomeValue16;
    private double ownedHomeValue17;
    private double ownedHomeValue18;
    private double ownedHomeValue19;
    private double totalRenters;
    private double rentValue0;
    private double rentValue1;
    private double rentValue2;
    private double rentValue3;
    private double rentValue4;
    private double rentValue5;
    private double rentValue6;
    private double rentValue7;
    private double rentValue8;
    private double rentValue9;
    private double rentValue10;
    private double rentValue11;
    private double rentValue12;
    private double rentValue13;
    private double rentValue14;
    private double rentValue15;
    private double rentValue16;
    private double totalRooms;
    private double elderlyPopulation;


    public MapMultiple() {}

    public MapMultiple(double own, double rent, double population, double maleNeverMarried, double femaleNeverMarried) {
        this.own = own;
        this.rent = rent;
        this.population = population;
        this.maleNeverMarried = maleNeverMarried;
        this.femaleNeverMarried = femaleNeverMarried;
    }

    //readFields and write must have the same order and contain all i/o variables!
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //q1
        own = dataInput.readDouble();
        rent = dataInput.readDouble();
        //q2
        population = dataInput.readDouble();
        maleNeverMarried = dataInput.readDouble();
        femaleNeverMarried = dataInput.readDouble();
        //q3
        hispanicMalesUnder18 = dataInput.readDouble();
        hispanicFemalesUnder18 = dataInput.readDouble();
        hispanicMales19to29 = dataInput.readDouble();
        hispanicFemales19to29 = dataInput.readDouble();
        hispanicMales30to39 = dataInput.readDouble();
        hispanicFemales30to39 = dataInput.readDouble();
        totalHispanicPopulation = dataInput.readDouble();
        //q4
        insideUrban = dataInput.readDouble();
        outsideUrban = dataInput.readDouble();
        rural = dataInput.readDouble();
        //q5
        totalOwnedHomes = dataInput.readDouble();
        ownedHomeValue0 = dataInput.readDouble();
        ownedHomeValue1 = dataInput.readDouble();
        ownedHomeValue2 = dataInput.readDouble();
        ownedHomeValue3 = dataInput.readDouble();
        ownedHomeValue4 = dataInput.readDouble();
        ownedHomeValue5 = dataInput.readDouble();
        ownedHomeValue6 = dataInput.readDouble();
        ownedHomeValue7 = dataInput.readDouble();
        ownedHomeValue8 = dataInput.readDouble();
        ownedHomeValue9 = dataInput.readDouble();
        ownedHomeValue10 = dataInput.readDouble();
        ownedHomeValue11 = dataInput.readDouble();
        ownedHomeValue12 = dataInput.readDouble();
        ownedHomeValue13 = dataInput.readDouble();
        ownedHomeValue14 = dataInput.readDouble();
        ownedHomeValue15 = dataInput.readDouble();
        ownedHomeValue16 = dataInput.readDouble();
        ownedHomeValue17 = dataInput.readDouble();
        ownedHomeValue18 = dataInput.readDouble();
        ownedHomeValue19 = dataInput.readDouble();
        //q6
        totalRenters = dataInput.readDouble();
        rentValue0 = dataInput.readDouble();
        rentValue1 = dataInput.readDouble();
        rentValue2 = dataInput.readDouble();
        rentValue3 = dataInput.readDouble();
        rentValue4 = dataInput.readDouble();
        rentValue5 = dataInput.readDouble();
        rentValue6 = dataInput.readDouble();
        rentValue7 = dataInput.readDouble();
        rentValue8 = dataInput.readDouble();
        rentValue9 = dataInput.readDouble();
        rentValue10 = dataInput.readDouble();
        rentValue11 = dataInput.readDouble();
        rentValue12 = dataInput.readDouble();
        rentValue13 = dataInput.readDouble();
        rentValue14 = dataInput.readDouble();
        rentValue15 = dataInput.readDouble();
        rentValue16 = dataInput.readDouble();
        //q7
        totalRooms = dataInput.readDouble();
        //q8
        elderlyPopulation = dataInput.readDouble();
        //q9
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //q1
        dataOutput.writeDouble(own);
        dataOutput.writeDouble(rent);
        //q2
        dataOutput.writeDouble(population);
        dataOutput.writeDouble(maleNeverMarried);
        dataOutput.writeDouble(femaleNeverMarried);
        //q3
        dataOutput.writeDouble(hispanicMalesUnder18);
        dataOutput.writeDouble(hispanicFemalesUnder18);
        dataOutput.writeDouble(hispanicMales19to29);
        dataOutput.writeDouble(hispanicFemales19to29);
        dataOutput.writeDouble(hispanicMales30to39);
        dataOutput.writeDouble(hispanicFemales30to39);
        dataOutput.writeDouble(totalHispanicPopulation);
        //q4
        dataOutput.writeDouble(insideUrban);
        dataOutput.writeDouble(outsideUrban);
        dataOutput.writeDouble(rural);
        //q5
        dataOutput.writeDouble(totalOwnedHomes);
        dataOutput.writeDouble(ownedHomeValue0);
        dataOutput.writeDouble(ownedHomeValue1);
        dataOutput.writeDouble(ownedHomeValue2);
        dataOutput.writeDouble(ownedHomeValue3);
        dataOutput.writeDouble(ownedHomeValue4);
        dataOutput.writeDouble(ownedHomeValue5);
        dataOutput.writeDouble(ownedHomeValue6);
        dataOutput.writeDouble(ownedHomeValue7);
        dataOutput.writeDouble(ownedHomeValue8);
        dataOutput.writeDouble(ownedHomeValue9);
        dataOutput.writeDouble(ownedHomeValue10);
        dataOutput.writeDouble(ownedHomeValue11);
        dataOutput.writeDouble(ownedHomeValue12);
        dataOutput.writeDouble(ownedHomeValue13);
        dataOutput.writeDouble(ownedHomeValue14);
        dataOutput.writeDouble(ownedHomeValue15);
        dataOutput.writeDouble(ownedHomeValue16);
        dataOutput.writeDouble(ownedHomeValue17);
        dataOutput.writeDouble(ownedHomeValue18);
        dataOutput.writeDouble(ownedHomeValue19);
        //q6
        dataOutput.writeDouble(totalRenters);
        dataOutput.writeDouble(rentValue0);
        dataOutput.writeDouble(rentValue1);
        dataOutput.writeDouble(rentValue2);
        dataOutput.writeDouble(rentValue3);
        dataOutput.writeDouble(rentValue4);
        dataOutput.writeDouble(rentValue5);
        dataOutput.writeDouble(rentValue6);
        dataOutput.writeDouble(rentValue7);
        dataOutput.writeDouble(rentValue8);
        dataOutput.writeDouble(rentValue9);
        dataOutput.writeDouble(rentValue10);
        dataOutput.writeDouble(rentValue11);
        dataOutput.writeDouble(rentValue12);
        dataOutput.writeDouble(rentValue13);
        dataOutput.writeDouble(rentValue14);
        dataOutput.writeDouble(rentValue15);
        dataOutput.writeDouble(rentValue16);
        //q7
        dataOutput.writeDouble(totalRooms);

        //q8
        dataOutput.writeDouble(elderlyPopulation);
        //q9
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
    public double getPopulation() {return population;}
    public void setPopulation(double population) {this.population = population;}

    //q3
    public double getHispanicMalesUnder18() {return hispanicMalesUnder18;}
    public void setHispanicMalesUnder18(double hispanicMalesUnder18) {this.hispanicMalesUnder18 = hispanicMalesUnder18;}
    public double getHispanicFemalesUnder18() {return hispanicFemalesUnder18;}
    public void setHispanicFemalesUnder18(double hispanicFemalesUnder18) {this.hispanicFemalesUnder18 = hispanicFemalesUnder18;}
    public double getHispanicMales19to29() {return hispanicMales19to29;}
    public void setHispanicMales19to29(double hispanicMales19to29) {this.hispanicMales19to29 = hispanicMales19to29;}
    public double getHispanicFemales19to29() {return hispanicFemales19to29;}
    public void setHispanicFemales19to29(double hispanicFemales19to29) {this.hispanicFemales19to29 = hispanicFemales19to29;}
    public double getHispanicMales30to39() {return hispanicMales30to39;}
    public void setHispanicMales30to39(double hispanicMales30to39) {this.hispanicMales30to39 = hispanicMales30to39;}
    public double getHispanicFemales30to39() {return hispanicFemales30to39;}
    public void setHispanicFemales30to39(double hispanicFemales30to39) {this.hispanicFemales30to39 = hispanicFemales30to39;}
    public double getTotalHispanicPopulation() {return totalHispanicPopulation;}
    public void setTotalHispanicPopulation(double totalHispanicPopulation) {this.totalHispanicPopulation = totalHispanicPopulation;}

    //q4
    public double getInsideUrban() {return insideUrban;}
    public void setInsideUrban(double insideUrban) {this.insideUrban = insideUrban;}
    public double getOutsideUrban() {return outsideUrban;}
    public void setOutsideUrban(double outsideUrban) {this.outsideUrban = outsideUrban;}
    public double getRural() {return rural;}
    public void setRural(double rural) {this.rural = rural;}

    //q5
    public double getTotalOwnedHomes() {return totalOwnedHomes;}
    public void setTotalOwnedHomes(double totalOwnedHomes) {this.totalOwnedHomes = totalOwnedHomes;}
    public double getOwnedHomeValue0() {return ownedHomeValue0;}
    public void setOwnedHomeValue0(double ownedHomeValue0) {this.ownedHomeValue0 = ownedHomeValue0;}
    public double getOwnedHomeValue1() {return ownedHomeValue1;}
    public void setOwnedHomeValue1(double ownedHomeValue1) {this.ownedHomeValue1 = ownedHomeValue1;}
    public double getOwnedHomeValue2() {return ownedHomeValue2;}
    public void setOwnedHomeValue2(double ownedHomeValue2) {this.ownedHomeValue2 = ownedHomeValue2;}
    public double getOwnedHomeValue3() {return ownedHomeValue3;}
    public void setOwnedHomeValue3(double ownedHomeValue3) {this.ownedHomeValue3 = ownedHomeValue3;}
    public double getOwnedHomeValue4() {return ownedHomeValue4;}
    public void setOwnedHomeValue4(double ownedHomeValue4) {this.ownedHomeValue4 = ownedHomeValue4;}
    public double getOwnedHomeValue5() {return ownedHomeValue5;}
    public void setOwnedHomeValue5(double ownedHomeValue5) {this.ownedHomeValue5 = ownedHomeValue5;}
    public double getOwnedHomeValue6() {return ownedHomeValue6;}
    public void setOwnedHomeValue6(double ownedHomeValue6) {this.ownedHomeValue6 = ownedHomeValue6;}
    public double getOwnedHomeValue7() {return ownedHomeValue7;}
    public void setOwnedHomeValue7(double ownedHomeValue7) {this.ownedHomeValue7 = ownedHomeValue7;}
    public double getOwnedHomeValue8() {return ownedHomeValue8;}
    public void setOwnedHomeValue8(double ownedHomeValue8) {this.ownedHomeValue8 = ownedHomeValue8;}
    public double getOwnedHomeValue9() {return ownedHomeValue9;}
    public void setOwnedHomeValue9(double ownedHomeValue9) {this.ownedHomeValue9 = ownedHomeValue9;}
    public double getOwnedHomeValue10() {return ownedHomeValue10;}
    public void setOwnedHomeValue10(double ownedHomeValue10) {this.ownedHomeValue10 = ownedHomeValue10;}
    public double getOwnedHomeValue11() {return ownedHomeValue11;}
    public void setOwnedHomeValue11(double ownedHomeValue11) {this.ownedHomeValue11 = ownedHomeValue11;}
    public double getOwnedHomeValue12() {return ownedHomeValue12;}
    public void setOwnedHomeValue12(double ownedHomeValue12) {this.ownedHomeValue12 = ownedHomeValue12;}
    public double getOwnedHomeValue13() {return ownedHomeValue13;}
    public void setOwnedHomeValue13(double ownedHomeValue13) {this.ownedHomeValue13 = ownedHomeValue13;}
    public double getOwnedHomeValue14() {return ownedHomeValue14;}
    public void setOwnedHomeValue14(double ownedHomeValue14) {this.ownedHomeValue14 = ownedHomeValue14;}
    public double getOwnedHomeValue15() {return ownedHomeValue15;}
    public void setOwnedHomeValue15(double ownedHomeValue15) {this.ownedHomeValue15 = ownedHomeValue15;}
    public double getOwnedHomeValue16() {return ownedHomeValue16;}
    public void setOwnedHomeValue16(double ownedHomeValue16) {this.ownedHomeValue16 = ownedHomeValue16;}
    public double getOwnedHomeValue17() {return ownedHomeValue17;}
    public void setOwnedHomeValue17(double ownedHomeValue17) {this.ownedHomeValue17 = ownedHomeValue17;}
    public double getOwnedHomeValue18() {return ownedHomeValue18;}
    public void setOwnedHomeValue18(double ownedHomeValue18) {this.ownedHomeValue18 = ownedHomeValue18;}
    public double getOwnedHomeValue19() {return ownedHomeValue19;}
    public void setOwnedHomeValue19(double ownedHomeValue19) {this.ownedHomeValue19 = ownedHomeValue19;}

    //q6
    public double getTotalRenters() {return totalRenters;}
    public void setTotalRenters(double totalRenters) {this.totalRenters = totalRenters;}
    public double getRentValue0() {return rentValue0;}
    public void setRentValue0(double rentValue0) {this.rentValue0 = rentValue0;}
    public double getRentValue1() {return rentValue1;}
    public void setRentValue1(double rentValue1) {this.rentValue1 = rentValue1;}
    public double getRentValue2() {return rentValue2;}
    public void setRentValue2(double rentValue2) {this.rentValue2 = rentValue2;}
    public double getRentValue3() {return rentValue3;}
    public void setRentValue3(double rentValue3) {this.rentValue3 = rentValue3;}
    public double getRentValue4() {return rentValue4;}
    public void setRentValue4(double rentValue4) {this.rentValue4 = rentValue4;}
    public double getRentValue5() {return rentValue5;}
    public void setRentValue5(double rentValue5) {this.rentValue5 = rentValue5;}
    public double getRentValue6() {return rentValue6;}
    public void setRentValue6(double rentValue6) {this.rentValue6 = rentValue6;}
    public double getRentValue7() {return rentValue7;}
    public void setRentValue7(double rentValue7) {this.rentValue7 = rentValue7;}
    public double getRentValue8() {return rentValue8;}
    public void setRentValue8(double rentValue8) {this.rentValue8 = rentValue8;}
    public double getRentValue9() {return rentValue9;}
    public void setRentValue9(double rentValue9) {this.rentValue9 = rentValue9;}
    public double getRentValue10() {return rentValue10;}
    public void setRentValue10(double rentValue10) {this.rentValue10 = rentValue10;}
    public double getRentValue11() {return rentValue11;}
    public void setRentValue11(double rentValue11) {this.rentValue11 = rentValue11;}
    public double getRentValue12() {return rentValue12;}
    public void setRentValue12(double rentValue12) {this.rentValue12 = rentValue12;}
    public double getRentValue13() {return rentValue13;}
    public void setRentValue13(double rentValue13) {this.rentValue13 = rentValue13;}
    public double getRentValue14() {return rentValue14;}
    public void setRentValue14(double rentValue14) {this.rentValue14 = rentValue14;}
    public double getRentValue15() {return rentValue15;}
    public void setRentValue15(double rentValue15) {this.rentValue15 = rentValue15;}
    public double getRentValue16() {return rentValue16;}
    public void setRentValue16(double rentValue16) {this.rentValue16 = rentValue16;}

    //q7
    public double getTotalRooms() {return totalRooms;}
    public void setTotalRooms(double totalRooms) {this.totalRooms = totalRooms;}

    //q8
    public double getElderlyPopulation() {return elderlyPopulation;}
    public void setElderlyPopulation(double elderlyPopulation) {this.elderlyPopulation = elderlyPopulation;}

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
        result = (int) (31 * result + population);
        result = 31 * result + (percentRent != null ? percentRent.hashCode() : 0);
        result = 31 * result + (percentOwn != null ? percentOwn.hashCode() : 0);
        return result;
    }
}
