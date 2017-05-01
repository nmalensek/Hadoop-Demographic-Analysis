package hadoop.data.analysis.ranges;

public class RentRanges implements Ranges {

    private static final RentRanges instance = new RentRanges();

    private RentRanges() {}

    public static RentRanges getInstance() {return instance;}

    String[] rent = {"Less than $100",
            "$100 - $149",
            "$150 - $199",
            "$200 - $249",
            "$250 - $299",
            "$300 - $349",
            "$350 - $399",
            "$400 - $449",
            "$450 - $499",
            "$500 - $549",
            "$550 - $599",
            "$600 - $649",
            "$650 - $699",
            "$700 - $749",
            "$750 - $999",
            "$1000 or more",
            "No cash rent"};
    //required for simple ordering for median calculation using TreeMap
    Integer[] integerRents =
            {0,
            100,
            150,
            200,
            250,
            300,
            350,
            400,
            450,
            500,
            550,
            600,
            650,
            700,
            750,
            1000,
            2000};

    public String[] getRanges() {return rent;}
    public Integer[] getIntegerRents() {return integerRents;}
}
