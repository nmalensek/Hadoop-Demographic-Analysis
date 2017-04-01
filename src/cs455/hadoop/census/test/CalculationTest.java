package cs455.hadoop.census.test;

import cs455.hadoop.census.ranges.HouseRanges;

import java.math.BigDecimal;
import java.text.DecimalFormat;
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
        testMap.put(houseRanges.getRanges()[0], 1000);
        testMap.put(houseRanges.getRanges()[1], 750);
        testMap.put(houseRanges.getRanges()[2], 2750);
        testMap.put(houseRanges.getRanges()[3], 2001);
        testMap.put(houseRanges.getRanges()[4], 4000);
        int total = 0;
        for (String key : testMap.keySet()) {
            total += testMap.get(key);
        }
        int half = total/2;
        int currentCount = 0;
        int iterations = 0;
        while (currentCount < half) {
                currentCount += testMap.get(houseRanges.getRanges()[iterations]);
                iterations++;
        }

        System.out.println(currentCount);
        System.out.println(houseRanges.getRanges()[iterations-1]);

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
