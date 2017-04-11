package cs455.hadoop.census.unused;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

        double ruralHouseholds = 0;
        double urbanHouseholds = 0;

        double totalHouses = 0;
        String homeValues = "";
        Double[] homeDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};

        double totalRenters = 0;
        String rentValues = "";
        Double[] rentDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};

        double dwellingsWithRooms = 0;
        String roomValues = "";
        Double[] averageRooms = new Double[9];

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

            intermediateStringData = cw.getQuestionFour().split(":");
            ruralHouseholds += Double.parseDouble(intermediateStringData[0]);
            urbanHouseholds += Double.parseDouble(intermediateStringData[1]);

            totalHouses += Double.parseDouble(cw.getQuestionFiveTotalHomes());
            intermediateStringData = cw.getQuestionFiveHomeValues().split(":");
            for (int i = 0; i < intermediateStringData.length-1; i++) {
                homeDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }

            totalRentals += Double.parseDouble(cw.getQuestionSixTotalRenters());
            intermediateStringData = cw.getQuestionSixRenterValues().split(":");
            for (int i = 0; i < intermediateStringData.length-1; i++) {
                rentDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }

            dwellingsWithRooms += Double.parseDouble(cw.getQuestionSevenDwellingsWithRooms());
            intermediateStringData = cw.getQuestionSevenRoomsPerHouse().split(":");
            for (int i = 0; i < intermediateStringData.length-1; i++) {
                averageRooms[i] = Double.parseDouble(intermediateStringData[i]) * i+1;
            }

        }

        //q1
        customWritable.setQuestionOne(totalRentals + ":" + totalOwners);
        //q2
        customWritable.setQuestionTwo(unmarriedMales + ":" + unmarriedFemales + ":" + totalPopulation);
        //q3
        customWritable.setQuestionThree(totalHispanicPopulation +":"+hispanicMalesUnder18+":"+hispanicMales19to29+
        ":"+hispanicMales30to39+":"+hispanicFemalesUnder18+":"+hispanicFemales19to29+":"+hispanicFemales30to39);
        //q4
        customWritable.setQuestionFour(ruralHouseholds+":"+urbanHouseholds);
        //q5
        customWritable.setQuestionFiveTotalHomes(String.valueOf(totalHouses));
        for (int i = 0; i < homeDoubles.length; i++) {
            homeValues += String.valueOf(homeDoubles[i] + ":");
        }
        customWritable.setQuestionFiveHomeValues(homeValues);
        //q6
        customWritable.setQuestionSixTotalRenters(String.valueOf(totalRenters));
        for (int i = 0; i < rentDoubles.length; i++) {
            rentValues += String.valueOf(rentDoubles[i] + ":");
        }
        customWritable.setQuestionSixRenterValues(rentValues);

        context.write(key, customWritable);
    }

}
