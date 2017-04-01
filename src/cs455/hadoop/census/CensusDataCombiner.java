package cs455.hadoop.census;

import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CensusDataCombiner extends Reducer<Text, MapMultiple, Text, MapMultiple> {

    @Override
    protected void reduce(Text key, Iterable<MapMultiple> values, Context context) throws IOException, InterruptedException {
        MapMultiple mapMultiple = new MapMultiple();

        double totalRent = 0;
        double totalOwn = 0;
        double totalMalesNeverMarried = 0;
        double totalFemalesNeverMarried = 0;
        double totalMarriageableMales = 0;
        double totalMarriageableFemales = 0;
        double insideUrban = 0;
        double outsideUrban = 0;
        double rural = 0;
        double hispanicMalesUnder18 = 0;
        double hispanicFemalesUnder18 = 0;
        double hispanicMales19to29 = 0;
        double hispanicFemales19to29 = 0;
        double hispanicMales30to39 = 0;
        double hispanicFemales30to39 = 0;
        double totalHispanicPopulation = 0;
        double totalOwnedHomes = 0;
        double ownedHomeValue0 = 0;
        double ownedHomeValue1 = 0;
        double ownedHomeValue2 = 0;
        double ownedHomeValue3 = 0;
        double ownedHomeValue4 = 0;
        double ownedHomeValue5 = 0;
        double ownedHomeValue6 = 0;
        double ownedHomeValue7 = 0;
        double ownedHomeValue8 = 0;
        double ownedHomeValue9 = 0;
        double ownedHomeValue10 = 0;
        double ownedHomeValue11 = 0;
        double ownedHomeValue12 = 0;
        double ownedHomeValue13 = 0;
        double ownedHomeValue14 = 0;
        double ownedHomeValue15 = 0;
        double ownedHomeValue16 = 0;
        double ownedHomeValue17 = 0;
        double ownedHomeValue18 = 0;
        double ownedHomeValue19 = 0;
        double totalRenters = 0;
        double rentValue0 = 0;
        double rentValue1 = 0;
        double rentValue2 = 0;
        double rentValue3 = 0;
        double rentValue4 = 0;
        double rentValue5 = 0;
        double rentValue6 = 0;
        double rentValue7 = 0;
        double rentValue8 = 0;
        double rentValue9 = 0;
        double rentValue10 = 0;
        double rentValue11 = 0;
        double rentValue12 = 0;
        double rentValue13 = 0;
        double rentValue14 = 0;
        double rentValue15 = 0;
        double rentValue16 = 0;

        for (MapMultiple val : values) {
            totalRent += val.getRent();
            totalOwn += val.getOwn();

            totalMalesNeverMarried += val.getMaleNeverMarried();
            totalFemalesNeverMarried += val.getFemaleNeverMarried();
            totalMarriageableMales += val.getMarriageableMales();
            totalMarriageableFemales += val.getMarriageableFemales();

            hispanicMalesUnder18 += val.getHispanicMalesUnder18();
            hispanicFemalesUnder18 += val.getHispanicFemalesUnder18();
            hispanicMales19to29 += val.getHispanicMales19to29();
            hispanicFemales19to29 += val.getHispanicFemales19to29();
            hispanicMales30to39 += val.getHispanicMales30to39();
            hispanicFemales30to39 += val.getHispanicFemales30to39();
            totalHispanicPopulation += val.getTotalHispanicPopulation();

            insideUrban += val.getInsideUrban();
            outsideUrban += val.getOutsideUrban();
            rural += val.getRural();

            totalOwnedHomes += val.getTotalOwnedHomes();
            ownedHomeValue0 += val.getOwnedHomeValue0();
            ownedHomeValue1 += val.getOwnedHomeValue1();
            ownedHomeValue2 += val.getOwnedHomeValue2();
            ownedHomeValue3 += val.getOwnedHomeValue3();
            ownedHomeValue4 += val.getOwnedHomeValue4();
            ownedHomeValue5 += val.getOwnedHomeValue5();
            ownedHomeValue6 += val.getOwnedHomeValue6();
            ownedHomeValue7 += val.getOwnedHomeValue7();
            ownedHomeValue8 += val.getOwnedHomeValue8();
            ownedHomeValue9 += val.getOwnedHomeValue9();
            ownedHomeValue10 += val.getOwnedHomeValue10();
            ownedHomeValue11 += val.getOwnedHomeValue11();
            ownedHomeValue12 += val.getOwnedHomeValue12();
            ownedHomeValue13 += val.getOwnedHomeValue13();
            ownedHomeValue14 += val.getOwnedHomeValue14();
            ownedHomeValue15 += val.getOwnedHomeValue15();
            ownedHomeValue16 += val.getOwnedHomeValue16();
            ownedHomeValue17 += val.getOwnedHomeValue17();
            ownedHomeValue18 += val.getOwnedHomeValue18();
            ownedHomeValue19 += val.getOwnedHomeValue19();

            totalRenters += val.getTotalRenters();
            rentValue0 += val.getRentValue0();
            rentValue1 += val.getRentValue1();
            rentValue2 += val.getRentValue2();
            rentValue3 += val.getRentValue3();
            rentValue4 += val.getRentValue4();
            rentValue5 += val.getRentValue5();
            rentValue6 += val.getRentValue6();
            rentValue7 += val.getRentValue7();
            rentValue8 += val.getRentValue8();
            rentValue9 += val.getRentValue9();
            rentValue10 += val.getRentValue10();
            rentValue11 += val.getRentValue11();
            rentValue12 += val.getRentValue12();
            rentValue13 += val.getRentValue13();
            rentValue14 += val.getRentValue14();
            rentValue15 += val.getRentValue15();
//            rentValue16 += val.getRentValue16();

        }

        //q1
        mapMultiple.setRent(totalRent);
        mapMultiple.setOwn(totalOwn);
        //q2
        mapMultiple.setMaleNeverMarried(totalMalesNeverMarried);
        mapMultiple.setFemaleNeverMarried(totalFemalesNeverMarried);
        mapMultiple.setMarriageableMales(totalMarriageableMales);
        mapMultiple.setMarriageableFemales(totalMarriageableFemales);
        //q3
        mapMultiple.setHispanicMalesUnder18(hispanicMalesUnder18);
        mapMultiple.setHispanicFemalesUnder18(hispanicFemalesUnder18);
        mapMultiple.setHispanicMales19to29(hispanicMales19to29);
        mapMultiple.setHispanicFemales19to29(hispanicFemales19to29);
        mapMultiple.setHispanicMales30to39(hispanicMales30to39);
        mapMultiple.setHispanicFemales30to39(hispanicFemales30to39);
        mapMultiple.setTotalHispanicPopulation(totalHispanicPopulation);
        //q4
        mapMultiple.setInsideUrban(insideUrban);
        mapMultiple.setOutsideUrban(outsideUrban);
        mapMultiple.setRural(rural);
        //q5
        mapMultiple.setTotalOwnedHomes(totalOwnedHomes);
        mapMultiple.setOwnedHomeValue0(ownedHomeValue0);
        mapMultiple.setOwnedHomeValue1(ownedHomeValue1);
        mapMultiple.setOwnedHomeValue2(ownedHomeValue2);
        mapMultiple.setOwnedHomeValue3(ownedHomeValue3);
        mapMultiple.setOwnedHomeValue4(ownedHomeValue4);
        mapMultiple.setOwnedHomeValue5(ownedHomeValue5);
        mapMultiple.setOwnedHomeValue6(ownedHomeValue6);
        mapMultiple.setOwnedHomeValue7(ownedHomeValue7);
        mapMultiple.setOwnedHomeValue8(ownedHomeValue8);
        mapMultiple.setOwnedHomeValue9(ownedHomeValue9);
        mapMultiple.setOwnedHomeValue10(ownedHomeValue10);
        mapMultiple.setOwnedHomeValue11(ownedHomeValue11);
        mapMultiple.setOwnedHomeValue12(ownedHomeValue12);
        mapMultiple.setOwnedHomeValue13(ownedHomeValue13);
        mapMultiple.setOwnedHomeValue14(ownedHomeValue14);
        mapMultiple.setOwnedHomeValue15(ownedHomeValue15);
        mapMultiple.setOwnedHomeValue16(ownedHomeValue16);
        mapMultiple.setOwnedHomeValue17(ownedHomeValue17);
        mapMultiple.setOwnedHomeValue18(ownedHomeValue18);
        mapMultiple.setOwnedHomeValue19(ownedHomeValue19);
        //q6
        mapMultiple.setTotalRenters(totalRenters);
        mapMultiple.setRentValue0(rentValue0);
        mapMultiple.setRentValue1(rentValue1);
        mapMultiple.setRentValue2(rentValue2);
        mapMultiple.setRentValue3(rentValue3);
        mapMultiple.setRentValue4(rentValue4);
        mapMultiple.setRentValue5(rentValue5);
        mapMultiple.setRentValue6(rentValue6);
        mapMultiple.setRentValue7(rentValue7);
        mapMultiple.setRentValue8(rentValue8);
        mapMultiple.setRentValue9(rentValue9);
        mapMultiple.setRentValue10(rentValue10);
        mapMultiple.setRentValue11(rentValue11);
        mapMultiple.setRentValue12(rentValue12);
        mapMultiple.setRentValue13(rentValue13);
        mapMultiple.setRentValue14(rentValue14);
        mapMultiple.setRentValue15(rentValue15);
        mapMultiple.setRentValue16(rentValue16);

        context.write(key, mapMultiple);
    }
}
