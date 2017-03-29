package cs455.hadoop.census;

import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */

public class CensusDataReducer extends Reducer<Text, MapMultiple, Text, Text> {
    MultipleOutputs multipleOutputs;

    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        multipleOutputs.write("question1", new Text("\nQuestion 1"), new Text(" "));
        multipleOutputs.write("question2", new Text("\nQuestion 2"), new Text(" "));
        multipleOutputs.write("question4", new Text("\nQuestion 4"), new Text(" "));
    }

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


        for (MapMultiple val : values) {
            totalRent += val.getRent();
            totalOwn += val.getOwn();
            totalMalesNeverMarried += val.getMaleNeverMarried();
            totalFemalesNeverMarried += val.getFemaleNeverMarried();
            totalMarriageableMales += val.getMarriageableMales();
            totalMarriageableFemales += val.getMarriageableFemales();
            insideUrban += val.getInsideUrban();
            outsideUrban += val.getOutsideUrban();
            rural += val.getRural();
        }

        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));
        multipleOutputs.write("question2", key, new Text(
                " Males never married: " +
                       calculatePercentage(totalMalesNeverMarried, (totalMarriageableMales))
                        + "% Females never married: " +
                        calculatePercentage(totalFemalesNeverMarried, (totalMarriageableFemales)) + "%"));
        multipleOutputs.write("question4", key, new Text(
                " Percent rural: "
                        + calculatePercentage(rural, (rural + insideUrban + outsideUrban)) +
                        "% Percent urban: " +
                        calculatePercentage((insideUrban + outsideUrban), (rural + insideUrban + outsideUrban))+ "%"));
//        multipleOutputs.write("question1", key, new Text(
//                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% own: "
//                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));
//        multipleOutputs.write("question2", key, new Text(
//                " Males never married: " +
//                        calculatePercentage(totalMalesNeverMarried, (totalMarriageableMales))
//                        + "% Females never married: " +
//                        calculatePercentage(totalFemalesNeverMarried, (totalMarriageableFemales)) + "%"));
//        multipleOutputs.write("question4", key, new Text(
//                " Percent rural: "
//                        + calculatePercentage(rural, (rural + insideUrban + outsideUrban)) +
//                        "% Percent urban: " +
//                        calculatePercentage((insideUrban + outsideUrban), (rural + insideUrban + outsideUrban)) + "%"));
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
}


//        mapMultiple.setPopulation(totalPopulation);
//        mapMultiple.setOwn(totalOwn);
//        mapMultiple.setRent(totalRent);
//        mapMultiple.setPercentRent(percentRent);
//        mapMultiple.setPercentOwn(percentOwn);

//        context.write(key, new Text(mapMultiple.toString()));