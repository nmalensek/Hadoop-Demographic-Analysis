package cs455.hadoop.census.unused;

import cs455.hadoop.census.ranges.HouseRanges;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class TextReducer extends Reducer<Text, CustomWritable, Text, Text> {
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
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> rentRangeMap = new HashMap<>();
        Map<Integer, Double> houseRangeMap = new TreeMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        double totalRent = 0;
        double totalOwn = 0;
        double totalPopulation = 0;
        double totalMalesNeverMarried = 0;
        double totalFemalesNeverMarried = 0;
        double totalHispanicPopulation = 0;
        double hispanicMalesUnder18 = 0;
        double hispanicFemalesUnder18 = 0;
        double hispanicMales19to29 = 0;
        double hispanicFemales19to29 = 0;
        double hispanicMales30to39 = 0;
        double hispanicFemales30to39 = 0;
        double ruralHouseholds = 0;
        double urbanHouseholds = 0;
        double totalHouses = 0;

        Double[] homeDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};

        for (CustomWritable cw : values) {
            totalRent += Double.parseDouble(cw.getQuestionOne().split(":")[0]);
            totalOwn += Double.parseDouble(cw.getQuestionOne().split(":")[1]);

            totalPopulation += Double.parseDouble(cw.getQuestionTwo().split(":")[0]);
            totalMalesNeverMarried += Double.parseDouble(cw.getQuestionTwo().split(":")[1]);
            totalFemalesNeverMarried += Double.parseDouble(cw.getQuestionTwo().split(":")[2]);

            totalHispanicPopulation += Double.parseDouble(cw.getQuestionThree().split(":")[0]);
            hispanicMalesUnder18 += Double.parseDouble(cw.getQuestionThree().split(":")[1]);
            hispanicMales19to29 += Double.parseDouble(cw.getQuestionThree().split(":")[2]);
            hispanicMales30to39 += Double.parseDouble(cw.getQuestionThree().split(":")[3]);
            hispanicFemalesUnder18 += Double.parseDouble(cw.getQuestionThree().split(":")[4]);
            hispanicFemales19to29 += Double.parseDouble(cw.getQuestionThree().split(":")[5]);
            hispanicFemales30to39 += Double.parseDouble(cw.getQuestionThree().split(":")[6]);

            ruralHouseholds += Double.parseDouble(cw.getQuestionFour().split(":")[0]);
            urbanHouseholds += Double.parseDouble(cw.getQuestionFour().split(":")[1]);

            totalHouses += Double.parseDouble(cw.getQuestionFiveTotalHomes());
            String[] intermediateStringData = cw.getQuestionFiveHomeValues().split(":");
            for (int i = 0; i < intermediateStringData.length-1; i++) {
                homeDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }
        }

        for (int i = 0; i < 20; i++) {
            houseRangeMap.put(houseRanges.getHousingIntegers()[i], homeDoubles[i]);
        }

        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));

        multipleOutputs.write("question2", key, new Text(
                " Males never married: " +
                        calculatePercentage(totalMalesNeverMarried, totalPopulation)
                        + "% Females never married: " +
                        calculatePercentage(totalFemalesNeverMarried, totalPopulation) + "%"));

        multipleOutputs.write("question3a", key, new Text(
                " Males: " + calculatePercentage(hispanicMalesUnder18, totalHispanicPopulation) +
                        "% | Females: " + calculatePercentage(hispanicFemalesUnder18, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question3b", key, new Text(
                " Males: " + calculatePercentage(hispanicMales19to29, totalHispanicPopulation) +
                        "% | Females: " + calculatePercentage(hispanicFemales19to29, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question3c", key, new Text(
                " Males: " + calculatePercentage(hispanicMales30to39, totalHispanicPopulation) +
                        "% | Females: " + calculatePercentage(hispanicFemales30to39, totalHispanicPopulation) +
                        "%"));

        multipleOutputs.write("question4", key, new Text(
                " Rural: " + calculatePercentage(ruralHouseholds, (ruralHouseholds + urbanHouseholds)) +
                        "% | Urban: " + calculatePercentage(urbanHouseholds, (ruralHouseholds + urbanHouseholds)) +
                        "%"));

        multipleOutputs.write("question5", key, new Text(
                " " + calculateMedian(houseRangeMap, HouseRanges.getInstance().getRanges(), totalHouses)));

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

    private String calculateMedian(Map<Integer, Double> map, String[] dataArray, double totalNumber) {
        int currentCount = 0;
        int iterations = 0;

        double dividingPoint = totalNumber * 0.50;

        for (Integer key : map.keySet()) {
            currentCount += map.get(key);
            iterations++;
            if (currentCount > dividingPoint) {
                break;
            }
        }

        String relevantRange = "N/A";

        if (iterations != 0) {
            relevantRange = dataArray[iterations - 1];
        }

        //debug
//        String test = "";
//        test += iterations + ":" + dividingPoint + ":" + totalNumber + "\n" + map.values().toString() + "\n";
//        for (Integer key : map.keySet()) {
//            test += "[";
//            test += key.toString() + ", ";
//            test += map.get(key) + "]\n";
//        }
//        test += "***" + relevantRange + "***";
        return relevantRange;
    }
}
