package hadoop.data.analysis.q9;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.axis.SymbolAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class YoungPopulationPlot {

    private String filePath;
    private StateStrings stateStrings = new StateStrings();
    private XYSeries ruralPopulation = new XYSeries("Rural population");
    private XYSeries childrenUnder12 = new XYSeries("Non-hispanic children <= 11");
    private XYSeries children12To17 = new XYSeries("Non-hispanic children 12 - 17");
    private XYSeries hispanicChildrenUnder12 = new XYSeries("Hispanic children <= 11");
    private XYSeries hispanicChildren12To17 = new XYSeries("Hispanic children 12-17");
    private XYSeries totalMales = new XYSeries("Total male population");
    private XYSeries totalFemales = new XYSeries("Total female population");
    private ApplicationFrame frame;
    private ApplicationFrame frameTwo;
    private ApplicationFrame frameThree;
    XYSeriesCollection data = new XYSeriesCollection();
    XYSeriesCollection hispanicData = new XYSeriesCollection();
    XYSeriesCollection genderData= new XYSeriesCollection();

    public YoungPopulationPlot(String inputFile) throws IOException {
        this.filePath = inputFile;
        frame = new ApplicationFrame("Rural population's influence on children's population");
        frameTwo = new ApplicationFrame("Rural population's influence on children's population");
        frameThree = new ApplicationFrame("Rural population's influence on male and female population");
    }

    /**
     * Checks for NA values that are inserted if average calculations return > 100% (now generally unused)
     * @throws IOException
     */

    private void checkForNA() throws IOException {
        BufferedReader checkReader = null;
        FileReader inputFileReader = new FileReader(filePath);
        try {

            checkReader = new BufferedReader(inputFileReader);
            String line = checkReader.readLine();

            while (line != null) {
                String[] split = line.split(":");
                if (split.length == 7) {
                    for (int i = 0; i < split.length; i++) {
                        if (split[i].equals("N/A")) {
                            split[i] = "0.0";
                        }
                    }
                }
                line = checkReader.readLine();
            }
        } finally {
            checkReader.close();
        }
    }

    /**
     * Creates plots for each XYCollection by reading the specified file
     * @throws IOException
     */

    public void plotOutput() throws IOException {
        BufferedReader reader = null;
        FileReader plotFileReader = new FileReader(filePath);
        try {

            reader = new BufferedReader(plotFileReader);
            String line = reader.readLine();

            while (line != null) {
                addDataToChart(line);
                line = reader.readLine();
            }
            addDataToSeries();
            createPlot(data, "Non-hispanic young population to rural population", frame);
            createPlot(hispanicData, "Hispanic young population to rural population", frameTwo);
            createPlot(genderData, "Male/female population to rural population", frameThree);
        } finally {
            reader.close();
            plotFileReader.close();
        }
    }

    /**
     * Populates all XYSeries with data from the read file
     * @param line
     */

    private void addDataToChart(String line) {
        String[] splitLine = line.split(":");
        //ignore lines without relevant data
        if (splitLine.length == 9) {
            try {
                for (int i = 0; i < splitLine.length; i++) {
                    if (!splitLine[0].equals("VI") && !splitLine[0].equals("PR")) {
                        ruralPopulation.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[2]));
                        childrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[3]));
                        children12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[4]));
                        hispanicChildrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[5]));
                        hispanicChildren12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[6]));
                        totalMales.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[7]));
                        totalFemales.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[8]));
                    }
                }
            } catch (NumberFormatException nfe) {
                //continue
            }
        }
    }

    /**
     * Adds each data series to their respective charts
     */

    private void addDataToSeries() {
        data.addSeries(ruralPopulation);
        data.addSeries(childrenUnder12);
        data.addSeries(children12To17);
        hispanicData.addSeries(ruralPopulation);
        hispanicData.addSeries(hispanicChildrenUnder12);
        hispanicData.addSeries(hispanicChildren12To17);
        genderData.addSeries(ruralPopulation);
        genderData.addSeries(totalMales);
        genderData.addSeries(totalFemales);
    }

    /**
     * Packages and displays the final charts
     * @param data data to be displayed in the chart
     * @param title title of chart
     * @param currentFrame frame that displays chart
     */

    private void createPlot(XYSeriesCollection data, String title, ApplicationFrame currentFrame) {

        JFreeChart chart = ChartFactory.createScatterPlot(title,
                "State", "Percentage", data,
                PlotOrientation.VERTICAL, true, true, false);

        XYPlot plot = (XYPlot) chart.getPlot();

        SymbolAxis xAxis = new SymbolAxis("State", stateStrings.getStates());
        xAxis.setTickUnit(new NumberTickUnit(1));
        xAxis.setRange(0, stateStrings.getStates().length - 1);
        plot.setDomainAxis(xAxis);

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
        currentFrame.setContentPane(chartPanel);
        currentFrame.pack();
        RefineryUtilities.centerFrameOnScreen(currentFrame);
        currentFrame.setVisible(true);
    }

    public static void main(String[] args) throws IOException {
        YoungPopulationPlot youngPopulationPlot = new YoungPopulationPlot(args[0]);
        youngPopulationPlot.checkForNA();
        youngPopulationPlot.plotOutput();
    }
}
