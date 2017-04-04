package cs455.hadoop.census.unused;

import cs455.hadoop.census.ranges.HouseRanges;
import cs455.hadoop.census.ranges.RentRanges;
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
        CustomWritable customWritable = new CustomWritable();
        Map<String, Double> houseRangeMap = new HashMap<>();
        Map<String, Double> rentRangeMap = new HashMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        RentRanges rentRanges = RentRanges.getInstance();
        double totalRent = 0;
        double totalOwn = 0;
        double totalPopulation = 0;
        double totalMalesNeverMarried = 0;
        double totalFemalesNeverMarried = 0;

        for (CustomWritable cw : values) {
            totalRent = Double.parseDouble(cw.getQuestionOne().split(":")[0]);
            totalOwn = Double.parseDouble(cw.getQuestionOne().split(":")[1]);

            totalPopulation = Double.parseDouble(cw.getQuestionTwo().split(":")[0]);
            totalMalesNeverMarried = Double.parseDouble(cw.getQuestionTwo().split(":")[1]);
            totalFemalesNeverMarried = Double.parseDouble(cw.getQuestionTwo().split(":")[2]);
        }


        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));

        multipleOutputs.write("question2", key, new Text(
                " Males never married: " +
                        calculatePercentage(totalMalesNeverMarried, totalPopulation)
                        + "% Females never married: " +
                        calculatePercentage(totalFemalesNeverMarried, totalPopulation) + "%"));

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

//    private String calculatePercentile(Map<String, Double> map, double totalNumber, double percentile) {
//        List<Double> sortedList = new ArrayList<>();
//        double TOTAL = 0;
//        int currentCount = 0;
//        int iterations = 0;
//
//        for (String key : map.keySet()) {
//            sortedList.add(map.get(key));
//            TOTAL += map.get(key);
//        }
//        Collections.sort(sortedList);
//        double dividingPoint = TOTAL * percentile;
//
//        while (currentCount < dividingPoint) {
//            currentCount += sortedList.get(iterations);
//            iterations++;
//        }
//
//        String relevantRange = "N/A";
//
//        if (iterations != 0) {
//            for (String key : map.keySet()) {
//                if (sortedList.get(iterations - 1) == map.get(key)) {
//                    relevantRange = key;
//                    break;
//                }
//            }
//        }
//
//        //debug
//        String test = "";
//        test += iterations + ":" + dividingPoint + ":" + totalNumber + ":" + TOTAL + "\n" + sortedList.toString() + "\n";
//        for (String key : map.keySet()) {
//            test += "[";
//            test += key.toString() + ", ";
//            test += map.get(key) + "]\n";
//        }
//        test += "***" + relevantRange + "***";
//        return test;
//
//
//    }
}
