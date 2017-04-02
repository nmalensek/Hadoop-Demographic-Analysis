package cs455.hadoop.census;

import cs455.hadoop.census.ranges.HouseRanges;
import cs455.hadoop.census.ranges.Ranges;
import cs455.hadoop.census.ranges.RentRanges;
import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */

public class CensusDataReducer extends Reducer<Text, MapMultiple, Text, Text> {
    private MultipleOutputs multipleOutputs;

    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        multipleOutputs.write("question1", new Text("\nQuestion 1"), new Text(" "));
        multipleOutputs.write("question2", new Text("\nQuestion 2"), new Text(" "));
        multipleOutputs.write("question3a", new Text("\nQuestion 3a"), new Text(" "));
        multipleOutputs.write("question3b", new Text("\nQuestion 3b"), new Text(" "));
        multipleOutputs.write("question3c", new Text("\nQuestion 3c"), new Text(" "));
        multipleOutputs.write("question4", new Text("\nQuestion 4"), new Text(" "));
        multipleOutputs.write("question5", new Text("\nQuestion 5"), new Text(" "));
        multipleOutputs.write("question6", new Text("\nQuestion 6"), new Text(" "));
    }

    @Override
    protected void reduce(Text key, Iterable<MapMultiple> values, Context context) throws IOException, InterruptedException {
        MapMultiple mapMultiple = new MapMultiple();
        Map<String, Double> houseRangeMap = new HashMap<>();
        Map<String, Double> rentRangeMap = new HashMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        RentRanges rentRanges = RentRanges.getInstance();

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
            rentValue16 += val.getRentValue16();
        }

        Double[] homeValueArray = {ownedHomeValue0, ownedHomeValue1, ownedHomeValue2, ownedHomeValue3,
                ownedHomeValue4, ownedHomeValue5, ownedHomeValue6, ownedHomeValue7, ownedHomeValue8, ownedHomeValue9,
                ownedHomeValue10, ownedHomeValue11, ownedHomeValue12, ownedHomeValue13, ownedHomeValue14,
                ownedHomeValue15, ownedHomeValue16, ownedHomeValue17, ownedHomeValue18, ownedHomeValue19};

        for (int i = 0; i < 20; i++) {
            houseRangeMap.put(houseRanges.getRanges()[i], homeValueArray[i]);
        }

        Double[] rentPaidArray = {rentValue0, rentValue1, rentValue2, rentValue3, rentValue4, rentValue5,
                rentValue6, rentValue7, rentValue8, rentValue9, rentValue10, rentValue11, rentValue12,
                rentValue13, rentValue14, rentValue15, rentValue16};

        for (int i = 0; i < 17; i++) {
            rentRangeMap.put(rentRanges.getRanges()[i], rentPaidArray[i]);
        }

        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));

        multipleOutputs.write("question2", key, new Text(
                " Males never married: " +
                        calculatePercentage(totalMalesNeverMarried, (totalMarriageableMales))
                        + "% Females never married: " +
                        calculatePercentage(totalFemalesNeverMarried, (totalMarriageableFemales)) + "%"));

        multipleOutputs.write("question3a", key, new Text(
                " Percent males <= 18: " + calculatePercentage(hispanicMalesUnder18, totalHispanicPopulation) +
                        "% Percent females <= 18: " + calculatePercentage(hispanicFemalesUnder18, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question3b", key, new Text(
                " Percent males 19 to 29: " + calculatePercentage(hispanicMales19to29, totalHispanicPopulation) +
                        "% Percent females 19 to 29: " + calculatePercentage(hispanicFemales19to29, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question3c", key, new Text(
                "Percent males 30 to 39: " + calculatePercentage(hispanicMales30to39, totalHispanicPopulation) +
                        "% Percent females 30 to 39: " + calculatePercentage(hispanicFemales30to39, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question4", key, new Text(
                " Percent rural: "
                        + calculatePercentage(rural, (rural + insideUrban + outsideUrban)) +
                        "% Percent urban: " +
                        calculatePercentage((insideUrban + outsideUrban), (rural + insideUrban + outsideUrban)) + "%"));

        multipleOutputs.write("question5", key, new Text(
                calculatePercentile(houseRangeMap, totalOwnedHomes, .50)));

        multipleOutputs.write("question6", key, new Text(
                calculatePercentile(rentRangeMap, totalRenters, .50)));

    }

    //must close multiple outputs, otherwise the results might not be written to output files
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        multipleOutputs.close();
    }

    private String calculatePercentage(double numerator, double denominator) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        double percentage = (numerator / denominator) * 100;
        if (Double.isInfinite(percentage)) {
            return "N/A";
        } else {
            return decimalFormat.format(percentage);
        }
    }

    private String calculatePercentile(Map<String, Double> map, double totalNumber, double percentile) {
        List<Double> sortedList = new ArrayList<>();
        double dividingPoint = totalNumber * percentile;
        int currentCount = 0;
        int iterations = 0;

        for (String key : map.keySet()) {
            sortedList.add(map.get(key));
        }
        Collections.sort(sortedList);

        while (currentCount < dividingPoint) {
            currentCount += sortedList.get(iterations);
            iterations++;
        }

        String relevantRange = "N/A";

        if (iterations != 0) {
            for (String key : map.keySet()) {
                if (sortedList.get(iterations - 1) == map.get(key)) {
                    relevantRange = key;
                }
            }
        }
        return relevantRange;
    }
}