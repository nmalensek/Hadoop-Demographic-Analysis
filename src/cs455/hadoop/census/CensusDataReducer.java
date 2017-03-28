package cs455.hadoop.census;

import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */

public class CensusDataReducer extends Reducer<Text, MapMultiple, Text, Text> {
    MultipleOutputs multipleOutputs;

    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        multipleOutputs.write("question1", new Text("Question 1"), new Text(" "));
        multipleOutputs.write("question2", new Text("Question 2"), new Text(" "));
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

        for(MapMultiple val : values){
            totalRent += val.getRent();
            totalOwn += val.getOwn();
            totalMalesNeverMarried += val.getMaleNeverMarried();
            totalFemalesNeverMarried += val.getFemaleNeverMarried();
            totalMarriageableMales += val.getMarriageableMales();
            totalMarriageableFemales += val.getMarriageableFemales();
        }

        BigDecimal percentRent = new BigDecimal(totalRent / (totalRent + totalOwn) * 100)
                .setScale(2, BigDecimal.ROUND_HALF_UP);
        BigDecimal percentOwn = new BigDecimal(totalOwn / (totalRent + totalOwn) * 100)
                .setScale(2, BigDecimal.ROUND_HALF_UP);

        BigDecimal percentMalesNeverMarried = new BigDecimal((totalMalesNeverMarried / totalMarriageableMales) * 100)
                .setScale(2, BigDecimal.ROUND_HALF_UP);
        BigDecimal percentFemalesNeverMarried = new BigDecimal((totalFemalesNeverMarried / totalMarriageableFemales) * 100)
                .setScale(2, BigDecimal.ROUND_HALF_UP);


        multipleOutputs.write("question1", key, new Text(
                " rent: " + percentRent.toString() + "% own: "
                        + percentOwn.toString() + "%"));
        multipleOutputs.write("question2", key, new Text(
                " Males never married: " + percentMalesNeverMarried.toString() + "% Females never married" +
                        ": " + percentFemalesNeverMarried.toString() + "%"));
    }
}


//        mapMultiple.setPopulation(totalPopulation);
//        mapMultiple.setOwn(totalOwn);
//        mapMultiple.setRent(totalRent);
//        mapMultiple.setPercentRent(percentRent);
//        mapMultiple.setPercentOwn(percentOwn);

//        context.write(key, new Text(mapMultiple.toString()));