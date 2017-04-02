package cs455.hadoop.census.test;

import cs455.hadoop.census.ranges.HouseRanges;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CalculationTest {

    private double AKRent = 82926;
    private double AKOwn = 105989;
    private int males1 = 5;
    private int males2 = 6;
    private int totalMales = 0;

    public CalculationTest() {

    }

    private void calcPercent() {
        BigDecimal percentRent =
                new BigDecimal((AKRent / (AKOwn + AKRent)) * 100).setScale(0, BigDecimal.ROUND_HALF_UP);
        BigDecimal percentOwn =
                new BigDecimal((AKOwn / (AKOwn + AKRent)) * 100).setScale(0, BigDecimal.ROUND_HALF_UP);
        System.out.println("AK: " + "Percentage residences rented: " + percentRent + "%"
                + " Percentage residences owned: " + percentOwn + "%");
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

    private void calcAdd() {
        totalMales = males1 + males2;
        System.out.println(totalMales);
    }

    private void femaleHispanicIterator() {
        int hispanicFemalesUnder18 = 0;
        int hispanicFemaleStartPosition = 4143;
        int positionPlusNine = 4152;
        for (int i = 0; i < 20; i++) {
            System.out.println(hispanicFemaleStartPosition + " - " + positionPlusNine);
            hispanicFemaleStartPosition = hispanicFemaleStartPosition + 9;
            positionPlusNine = positionPlusNine + 9;
        }
    }

    private void medianTest() {
        Map<String, Integer> testMap = new HashMap<>();
        HouseRanges houseRanges = HouseRanges.getInstance();
        ArrayList<Integer> sortedList = new ArrayList<>();
        testMap.put(houseRanges.getRanges()[0], 2001);
        testMap.put(houseRanges.getRanges()[1], 750);
        testMap.put(houseRanges.getRanges()[2], 4750);
        testMap.put(houseRanges.getRanges()[3], 1000);
        testMap.put(houseRanges.getRanges()[4], 2000);

        for (String key : testMap.keySet()) {
            sortedList.add(testMap.get(key));
        }
        Collections.sort(sortedList);
        System.out.println(sortedList);
        int total = 0;
        for (int i = 0; i < sortedList.size(); i++) {
            total += sortedList.get(i);
        }
        double half = total * .50;
        int currentCount = 0;
        int iterations = 0;
        while (currentCount < half) {
                currentCount += sortedList.get(iterations);
                iterations++;
        }

        String median = "";
        for (String key : testMap.keySet()) {
            if (sortedList.get(iterations-1) == testMap.get(key)) {
                median = key;
            }
        }

        System.out.println(currentCount);
        System.out.println(median);

    }


    public static void main(String[] args) {
        CalculationTest calculationTest = new CalculationTest();
//        calculationTest.calcPercent();
//        calculationTest.calcAdd();
//        System.out.println(calculationTest.calculatePercentage(1, 3));
//        System.out.println(calculationTest.calculatePercentage(5, 10));
//        System.out.println(calculationTest.calculatePercentage(10, 0));
//        calculationTest.femaleHispanicIterator();
        calculationTest.medianTest();
    }


}
