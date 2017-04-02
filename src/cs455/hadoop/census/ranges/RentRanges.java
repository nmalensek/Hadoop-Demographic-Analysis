package cs455.hadoop.census.ranges;

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

    public String[] getRanges() {return rent;}
}