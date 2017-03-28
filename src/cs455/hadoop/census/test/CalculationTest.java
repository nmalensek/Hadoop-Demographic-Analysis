package cs455.hadoop.census.test;

import java.math.BigDecimal;

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

    private void calcAdd() {
        totalMales = males1 + males2;
        System.out.println(totalMales);
    }


    public static void main(String[] args) {
        CalculationTest calculationTest = new CalculationTest();
        calculationTest.calcPercent();
        calculationTest.calcAdd();
    }


}
