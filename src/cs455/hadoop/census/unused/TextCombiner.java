package cs455.hadoop.census.unused;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TextCombiner extends Reducer<Text, CustomWritable, Text, CustomWritable> {

    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        CustomWritable customWritable = new CustomWritable();

        String[] intermediateStringData;
        String[] qTwoArray;
        double totalPopulation = 0;
        double unmarriedMales = 0;
        double unmarriedFemales = 0;
        double totalRentals = 0;
        double totalOwners = 0;

        for (CustomWritable cw : values) {

            intermediateStringData = cw.getQuestionOne().split(":");
            totalRentals += Double.parseDouble(intermediateStringData[0]);
            totalOwners += Double.parseDouble(intermediateStringData[1]);

            qTwoArray = cw.getQuestionTwo().split(":");
            unmarriedMales += Double.parseDouble(qTwoArray[0]);
            unmarriedFemales += Double.parseDouble(qTwoArray[1]);
            totalPopulation += Double.parseDouble(qTwoArray[2]);
        }

        //q1
        customWritable.setQuestionOne(totalRentals + ":" + totalOwners);
        //q2
        customWritable.setQuestionTwo(unmarriedMales + ":" + unmarriedFemales + ":" + totalPopulation);

        context.write(key, customWritable);
    }

}
