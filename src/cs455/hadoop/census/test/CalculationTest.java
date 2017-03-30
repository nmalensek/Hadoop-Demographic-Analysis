package cs455.hadoop.census.test;

import java.math.BigDecimal;
import java.text.DecimalFormat;

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


    public static void main(String[] args) {
        CalculationTest calculationTest = new CalculationTest();
//        calculationTest.calcPercent();
//        calculationTest.calcAdd();
//        System.out.println(calculationTest.calculatePercentage(1, 3));
//        System.out.println(calculationTest.calculatePercentage(5, 10));
//        System.out.println(calculationTest.calculatePercentage(10, 0));
        calculationTest.femaleHispanicIterator();
    }


}
