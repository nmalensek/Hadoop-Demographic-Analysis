package cs455.hadoop.census.original;

import cs455.hadoop.census.ranges.HouseRanges;
import cs455.hadoop.census.ranges.RentRanges;
import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class CensusDataReducer extends Reducer<Text, MapMultiple, Text, Text> {
    private MultipleOutputs multipleOutputs;
    private Map<Text, Double> elderlyMap = new HashMap<>();
    private Text mostElderlyState = new Text();
    private List<Double> averageList = new ArrayList<>();
    private double currentMax = 0;

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
                        "Does the amount of urban and rural population influence the population of children < 17 per state?"),
                new Text(" \n"));
    }

    @Override
    protected void reduce(Text key, Iterable<MapMultiple> values, Context context) throws IOException, InterruptedException {
        MapMultiple mapMultiple = new MapMultiple();
        Map<Integer, Double> houseRangeMap = new TreeMap<>();
        Map<Integer, Double> rentRangeMap = new TreeMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        RentRanges rentRanges = RentRanges.getInstance();

        double totalRent = 0;
        double totalOwn = 0;
        double population = 0;
        double totalMalesNeverMarried = 0;
        double totalFemalesNeverMarried = 0;
        double insideUrban = 0;
        double outsideUrban = 0;
        double rural = 0;
        double notDefined = 0;
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

        double totalRooms = 0;
        double oneRoom = 0;
        double twoRooms = 0;
        double threeRooms = 0;
        double fourRooms = 0;
        double fiveRooms = 0;
        double sixRooms = 0;
        double sevenRooms = 0;
        double eightRooms = 0;
        double nineRooms = 0;
        double averageRooms = 0;

        double elderlyPopulation = 0;

        double urbanPopulation = 0;
        double ruralPopulation = 0;
        double childrenUnder1To11 = 0;
        double children12To17 = 0;
        double hispanicChildrenUnder1To11 = 0;
        double hispanicChildren12To17 = 0;

        for (MapMultiple val : values) {
            totalRent += val.getRent();
            totalOwn += val.getOwn();

            population += val.getPopulation();
            totalMalesNeverMarried += val.getMaleNeverMarried();
            totalFemalesNeverMarried += val.getFemaleNeverMarried();

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
            notDefined += val.getNotDefined();

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

            totalRooms += val.getTotalRooms();
            oneRoom += val.getOneRoom();
            twoRooms += val.getTwoRooms();
            threeRooms += val.getThreeRooms();
            fourRooms += val.getFourRooms();
            fiveRooms += val.getFiveRooms();
            sixRooms += val.getSixRooms();
            sevenRooms += val.getSevenRooms();
            eightRooms += val.getEightRooms();
            nineRooms += val.getNineRooms();

            elderlyPopulation += val.getElderlyPopulation();
            elderlyMap.put(key, Double.parseDouble(calculatePercentage(elderlyPopulation, population)));

            urbanPopulation += val.getUrbanPopulation();
            ruralPopulation += val.getRuralPopulation();
            childrenUnder1To11 += val.getChildrenUnder1To11();
            children12To17 += val.getChildren12To17();
            hispanicChildrenUnder1To11 += val.getHispanicChildrenUnder1To11();
            hispanicChildren12To17 += val.getHispanicChildren12To17();
        }

        Double[] roomArray = {oneRoom * 1, twoRooms * 2, threeRooms * 3, fourRooms * 4, fiveRooms * 5,
                sixRooms * 6, sevenRooms * 7, eightRooms * 8, nineRooms * 9};

        DecimalFormat dF = new DecimalFormat("##.00");
        double average = calculateAverageRooms(roomArray, totalRooms);
        if (!Double.isNaN(average)) {
            double formattedAverage = Double.parseDouble(dF.format(average));
            averageRooms = formattedAverage;
        } else {
            averageRooms = 0;
        }

        if (averageRooms != 0) {
            averageList.add(averageRooms);
        }

        Double[] homeValueArray = {ownedHomeValue0, ownedHomeValue1, ownedHomeValue2, ownedHomeValue3,
                ownedHomeValue4, ownedHomeValue5, ownedHomeValue6, ownedHomeValue7, ownedHomeValue8, ownedHomeValue9,
                ownedHomeValue10, ownedHomeValue11, ownedHomeValue12, ownedHomeValue13, ownedHomeValue14,
                ownedHomeValue15, ownedHomeValue16, ownedHomeValue17, ownedHomeValue18, ownedHomeValue19};

        for (int i = 0; i < 20; i++) {
            houseRangeMap.put(houseRanges.getHousingIntegers()[i], homeValueArray[i]);
        }

        Double[] rentPaidArray = {rentValue0, rentValue1, rentValue2, rentValue3, rentValue4, rentValue5,
                rentValue6, rentValue7, rentValue8, rentValue9, rentValue10, rentValue11, rentValue12,
                rentValue13, rentValue14, rentValue15, rentValue16};

        for (int i = 0; i < 17; i++) {
            rentRangeMap.put(rentRanges.getIntegerRents()[i], rentPaidArray[i]);
        }

        multipleOutputs.write("question1", key, new Text(
                " rent: " + calculatePercentage(totalRent, (totalRent + totalOwn)) + "% | own: "
                        + calculatePercentage(totalOwn, (totalRent + totalOwn)) + "%"));

        multipleOutputs.write("question2", key, new Text(
                " Males: " +
                        calculatePercentage(totalMalesNeverMarried, (population))
                        + "% | Females: " +
                        calculatePercentage(totalFemalesNeverMarried, (population)) + "%"));

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
                " Rural: "
                        + calculatePercentage(rural, (rural + insideUrban + outsideUrban + notDefined)) +
                        "% | Urban: " +
                        calculatePercentage((insideUrban + outsideUrban), (rural + insideUrban + outsideUrban + notDefined)) + "%"));

        multipleOutputs.write("question5", key, new Text(
                " " + calculateMedian(houseRangeMap, houseRanges.getRanges(), totalOwnedHomes)));

        multipleOutputs.write("question6", key, new Text(
                " " + calculateMedian(rentRangeMap, rentRanges.getRanges(), totalRenters)));

        multipleOutputs.write("question9", key, new Text(
                calculatePercentage(urbanPopulation, population) + ":" +
                        calculatePercentage(ruralPopulation, population) +
                        ":" + calculatePercentage(childrenUnder1To11, population) +
                        ":" + calculatePercentage(children12To17, population) +
                        ":" + calculatePercentage(hispanicChildrenUnder1To11, population) +
                        ":" + calculatePercentage(hispanicChildren12To17, population)));

        stateWithMostElderlyPeople(elderlyMap);
    }

    //must close multiple outputs, otherwise the results might not be written to output files
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //question 7 and 8 written here so only one value's output
        multipleOutputs.write("question7", "", new Text(calculateNinetyFifthPercentile(averageList) +
                averageList + " rooms"));
        multipleOutputs.write("question8", mostElderlyState, new Text(
                " " + currentMax + "%"));
        super.cleanup(context);
        multipleOutputs.close();
    }

    private String calculatePercentage(double numerator, double denominator) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        double percentage = (numerator / denominator) * 100;
        if (Double.isInfinite(percentage) || percentage > 100 || percentage < 0) {
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

    private double calculateAverageRooms(Double[] rooms, double totalHouses) {
        double actualRoomQuantity = 0;
        for (int i = 0; i < 9; i++) {
            actualRoomQuantity += rooms[i];
        }
        return  actualRoomQuantity / totalHouses;
    }

    private void stateWithMostElderlyPeople(Map<Text, Double> stateElderlyMap) {
        for (Text state : stateElderlyMap.keySet()) {
            if (stateElderlyMap.get(state) > currentMax) {
                currentMax = stateElderlyMap.get(state);
                mostElderlyState.set(state);
            }
        }
    }

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
}