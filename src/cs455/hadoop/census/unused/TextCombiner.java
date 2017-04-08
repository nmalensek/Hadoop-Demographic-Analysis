package cs455.hadoop.census.unused;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TextCombiner extends Reducer<Text, CustomWritable, Text, CustomWritable> {

    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        CustomWritable customWritable = new CustomWritable();

        String[] intermediateStringData;
        double totalPopulation = 0;
        double unmarriedMales = 0;
        double unmarriedFemales = 0;
        double totalRentals = 0;
        double totalOwners = 0;
        double totalHispanicPopulation = 0;
        double hispanicMalesUnder18 = 0;
        double hispanicFemalesUnder18 = 0;
        double hispanicMales19to29 = 0;
        double hispanicFemales19to29 = 0;
        double hispanicMales30to39 = 0;
        double hispanicFemales30to39 = 0;

        for (CustomWritable cw : values) {

            intermediateStringData = cw.getQuestionOne().split(":");
            totalRentals += Double.parseDouble(intermediateStringData[0]);
            totalOwners += Double.parseDouble(intermediateStringData[1]);

            intermediateStringData = cw.getQuestionTwo().split(":");
            unmarriedMales += Double.parseDouble(intermediateStringData[0]);
            unmarriedFemales += Double.parseDouble(intermediateStringData[1]);
            totalPopulation += Double.parseDouble(intermediateStringData[2]);

            intermediateStringData = cw.getQuestionThree().split(":");
            totalHispanicPopulation += Double.parseDouble(intermediateStringData[0]);
            hispanicMalesUnder18 += Double.parseDouble(intermediateStringData[1]);
            hispanicMales19to29 += Double.parseDouble(intermediateStringData[2]);
            hispanicMales30to39 += Double.parseDouble(intermediateStringData[3]);
            hispanicFemalesUnder18 += Double.parseDouble(intermediateStringData[4]);
            hispanicFemales19to29 += Double.parseDouble(intermediateStringData[5]);
            hispanicFemales30to39 += Double.parseDouble(intermediateStringData[6]);
        }

        //q1
        customWritable.setQuestionOne(totalRentals + ":" + totalOwners);
        //q2
        customWritable.setQuestionTwo(unmarriedMales + ":" + unmarriedFemales + ":" + totalPopulation);
        //q3
        customWritable.setQuestionThree(totalHispanicPopulation +":"+hispanicMalesUnder18+":"+hispanicMales19to29+
        ":"+hispanicMales30to39+":"+hispanicFemalesUnder18+":"+hispanicFemales19to29+":"+hispanicFemales30to39);

        context.write(key, customWritable);
    }

}
