package cs455.hadoop.census.text;

import cs455.hadoop.census.ranges.HouseRanges;
import cs455.hadoop.census.ranges.RentRanges;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class TextReducer extends Reducer<Text, CustomWritable, Text, Text> {
    private MultipleOutputs multipleOutputs;
    private List<Double> averageList = new ArrayList<>();
    private Map<Text, Double> elderlyMap = new HashMap<>();
    private Text mostElderlyState = new Text();
    private double currentMax = 0;

    /**
     * Writes answers to each question in their own files.
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        multipleOutputs.write("question1", new Text("\nQuestion 1:\n" +
                "Percentage of residences rented vs. owned"), new Text(" \n"));
        multipleOutputs.write("question2", new Text("\nQuestion 2:\n" +
                "Percentage of males and females of the state population that never married"), new Text(" \n"));
        multipleOutputs.write("question3a", new Text("\nQuestion 3a:\n" +
                "Percentage of hispanic population <= 18 years old"), new Text(" \n"));
        multipleOutputs.write("question3b", new Text("\nQuestion 3b:\n" +
                "Percentage of hispanic population >= 19 and <= 29"), new Text(" \n"));
        multipleOutputs.write("question3c", new Text("\nQuestion 3c:\n" +
                "Percentage of hispanic population >= 30 and <= 39"), new Text(" \n"));
        multipleOutputs.write("question4", new Text("\nQuestion 4:\n" +
                "Percentage of rural households vs. urban households"), new Text(" \n"));
        multipleOutputs.write("question5", new Text("\nQuestion 5:\n" +
                "Median value of houses occupied by owners"), new Text(" \n"));
        multipleOutputs.write("question6", new Text("\nQuestion 6:\n" +
                "Median rent paid by households"), new Text(" \n"));
        multipleOutputs.write("question7", new Text("\nQuestion 7:\n" +
                "95th percentile of the average number of rooms per house"), new Text(" \n"));
        multipleOutputs.write("question8", new Text("\nQuestion 8:\n" +
                "State that has the highest percentage of people aged > 85"), new Text(" \n"));
        multipleOutputs.write("question9", new Text("\nQuestion 9:\n" +
                        "Does the amount of urban and rural population influence the population of children < 17 or " +
                        "the number of males/females per state?"),
                new Text(" \n"));
    }

    /**
     * Sums all values and sets the final values for each variable. Performs calculations as necessary and
     * writes to output file.
     * @param key state
     * @param values MapMultiple objects that contain values for each state
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> rentRangeMap = new TreeMap<>();
        Map<Integer, Double> houseRangeMap = new TreeMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        RentRanges rentRanges = RentRanges.getInstance();
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
        double totalRenters = 0;
        double totalRooms = 0;
        double averageRooms = 0;
        double elderlyPopulation = 0;

        double urbanPopulation = 0;
        double ruralPopulation = 0;
        double childrenUnder1To11 = 0;
        double children12To17 = 0;
        double hispanicChildrenUnder1To11 = 0;
        double hispanicChildren12To17 = 0;
        double totalMales = 0;
        double totalFemales = 0;

        Double[] homeDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
        Double[] rentDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
        Double[] roomDoubles = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};

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
            for (int i = 0; i < intermediateStringData.length; i++) {
                homeDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }

            totalRenters += Double.parseDouble(cw.getQuestionSixTotalRenters());
            intermediateStringData = cw.getQuestionSixRenterValues().split(":");
            for (int i = 0; i < intermediateStringData.length; i++) {
                rentDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }

            totalRooms += Double.parseDouble(cw.getQuestionSevenDwellingsWithRooms());
            intermediateStringData = cw.getQuestionSevenRoomsPerHouse().split(":");
            for (int i = 0; i < intermediateStringData.length; i++) {
                roomDoubles[i] += Double.parseDouble(intermediateStringData[i]);
            }

            elderlyPopulation += Double.parseDouble(cw.getQuestionEight().split(":")[0]);
            elderlyMap.put(key, Double.parseDouble(calculatePercentage(elderlyPopulation, totalPopulation)));

            urbanPopulation += Double.parseDouble(cw.getQuestionNine().split(":")[0]);
            ruralPopulation += Double.parseDouble(cw.getQuestionNine().split(":")[1]);
            childrenUnder1To11 += Double.parseDouble(cw.getQuestionNine().split(":")[2]);
            children12To17 += Double.parseDouble(cw.getQuestionNine().split(":")[3]);
            hispanicChildrenUnder1To11 += Double.parseDouble(cw.getQuestionNine().split(":")[4]);
            hispanicChildren12To17 += Double.parseDouble(cw.getQuestionNine().split(":")[5]);
            totalMales += Double.parseDouble(cw.getQuestionNine().split(":")[6]);
            totalFemales += Double.parseDouble(cw.getQuestionNine().split(":")[7]);

        }

        //put home values into an array so they can be put into a map with the ranges
        for (int i = 0; i < 20; i++) {
            houseRangeMap.put(houseRanges.getHousingIntegers()[i], homeDoubles[i]);
        }

        //put rent values into an array so they can be put into a map with the ranges
        for (int i = 0; i < 17; i++) {
            rentRangeMap.put(rentRanges.getIntegerRents()[i], rentDoubles[i]);
        }

        //multiply rooms to get total rooms in state for average calculation
        for (int i = 0; i < roomDoubles.length; i++) {
            roomDoubles[i] = (roomDoubles[i] * (i+1));
        }

        DecimalFormat dF = new DecimalFormat("##.00");
        double average = calculateAverageRooms(roomDoubles, totalRooms);
        if (!Double.isNaN(average) && !Double.isInfinite(average)) {
            double formattedAverage = Double.parseDouble(dF.format(average));
            averageRooms = formattedAverage;
        } else {
            averageRooms = 0;
        }

        if (averageRooms != 0) {
            averageList.add(averageRooms);
        }

        //write answers for each state

        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% | own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));

        multipleOutputs.write("question2", key, new Text(
                " Males: " +
                        calculatePercentage(totalMalesNeverMarried, totalPopulation)
                        + "% | Females: " +
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
                " " + calculateMedian(houseRangeMap, houseRanges.getRanges(), totalHouses)));

        multipleOutputs.write("question6", key, new Text(
                " " + calculateMedian(rentRangeMap, rentRanges.getRanges(), totalRenters)));

        multipleOutputs.write("question9", key, new Text(
                calculatePercentage(urbanPopulation, totalPopulation) + ":" +
                        calculatePercentage(ruralPopulation, totalPopulation) +
                        ":" + calculatePercentage(childrenUnder1To11, totalPopulation) +
                        ":" + calculatePercentage(children12To17, totalPopulation) +
                        ":" + calculatePercentage(hispanicChildrenUnder1To11, totalPopulation) +
                        ":" + calculatePercentage(hispanicChildren12To17, totalPopulation) +
                        ":" + calculatePercentage(totalMales, totalPopulation) +
                        ":" + calculatePercentage(totalFemales, totalPopulation)));

        stateWithMostElderlyPeople(elderlyMap);
    }

    /**
     * Close multiple outputs, otherwise the results might not be written to output files.
     * Also writes questions 7 and 8 because the answer only contains one data point instead of one
     * for each state.
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.write("question7", "", new Text(
                calculateNinetyFifthPercentile(averageList) + " rooms"));
        multipleOutputs.write("question8", mostElderlyState, new Text(
                " " + currentMax + "%"));
        super.cleanup(context);
        multipleOutputs.close();
    }

    /**
     * Calculate percentage, ignores answer if impossible number is calculated (VI and PR
     * generally cause this)
     * @param numerator
     * @param denominator
     * @return
     */

    private String calculatePercentage(double numerator, double denominator) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        double percentage = (numerator / denominator) * 100;
        if (Double.isInfinite(percentage) || percentage > 100 || percentage < 0) {
            return "N/A";
        } else {
            return decimalFormat.format(percentage);
        }
    }


    /**
     * Calculates median, returns N/A if no iterations were performed (no data was collected).
     * The current count is tracked because this is calculating the median from ranges, not from
     * each data point.
     * @param map map of ranges (key) and quantity per range (value)
     * @param dataArray array of ranges
     * @param totalNumber total number of the variable that's being examined (home values or rent ranges)
     * @return answer
     */

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

//        //debug
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

    /**
     * Calculates 95th percentile of the given list. If the result of list * .95 divides evenly,
     * that number is the 95th percentile. Otherwise, the next result is in the 95th percentile.
     * @param list list to calculate 95th percentile from
     * @return
     */

    private String calculateNinetyFifthPercentile(List<Double> list) {
        Collections.sort(list);
        BigDecimal ninetyFifthPercentile = null;

        double rawPercentile = list.size() * 0.95;

        if (rawPercentile % 1 == 0) {
            ninetyFifthPercentile = new BigDecimal(rawPercentile).setScale(0);
        }
        if (rawPercentile % 1 != 0) {
            ninetyFifthPercentile = new BigDecimal(rawPercentile).setScale(0, BigDecimal.ROUND_UP);
        }
        int ninetyFifthPercentilePosition = ninetyFifthPercentile.intValueExact();

        double ninetyFifthPercentileNumber = list.get(ninetyFifthPercentilePosition - 1);

        String answer = Double.toString(ninetyFifthPercentileNumber);
//        debug
//        String test = "";
//        test += ninetyFifthPercentile + ":" + ninetyFifthPercentilePosition + "\n" + list.toString() + "\n";
//        test += list.size() + "\n";
//        test += "***" + ninetyFifthPercentileNumber + "***";

        return answer;
    }

    private double calculateAverageRooms(Double[] rooms, double totalHouses) {
        double actualRoomQuantity = 0;
        for (int i = 0; i < 9; i++) {
            actualRoomQuantity += rooms[i];
        }
        return  actualRoomQuantity / totalHouses;
    }

    /**
     * Checks if the percentage of elderly population in the state is the most compared to all other
     * states analyzed so far.
     * @param stateElderlyMap Map of states' elderly population percentages
     */


    private void stateWithMostElderlyPeople(Map<Text, Double> stateElderlyMap) {
        for (Text state : stateElderlyMap.keySet()) {
            if (stateElderlyMap.get(state) > currentMax) {
                currentMax = stateElderlyMap.get(state);
                mostElderlyState.set(state);
            }
        }
    }
}
