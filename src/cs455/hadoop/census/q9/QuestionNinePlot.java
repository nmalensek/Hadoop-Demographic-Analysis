package cs455.hadoop.census.q9;

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

public class QuestionNinePlot {

    private String filePath;
    private StateStrings stateStrings = new StateStrings();
    private XYSeries ruralPopulation = new XYSeries("Rural population");
    private XYSeries childrenUnder12 = new XYSeries("Non-hispanic children <= 11");
    private XYSeries children12To17 = new XYSeries("Non-hispanic children 12 - 17");
    private XYSeries hispanicChildrenUnder12 = new XYSeries("Hispanic children <= 11");
    private XYSeries hispanicChildren12To17 = new XYSeries("Hispanic children 12-17");
    private ApplicationFrame frame;
    private ApplicationFrame frameTwo;
    XYSeriesCollection data = new XYSeriesCollection();
    XYSeriesCollection hispanicData = new XYSeriesCollection();

    public QuestionNinePlot(String inputFile) throws IOException {
        this.filePath = inputFile;
        frame = new ApplicationFrame("Rural population's influence on children's population");
        frameTwo = new ApplicationFrame("Rural population's influence on children's population");
    }

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
        } finally {
            reader.close();
            plotFileReader.close();
        }
    }

    private void addDataToChart(String line) {
        String[] splitLine = line.split(":");
        //ignore lines without relevant data
        if (splitLine.length == 7) {
            try {
                for (int i = 0; i < splitLine.length; i++) {
                    ruralPopulation.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[2]));
                    childrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[3]));
                    children12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[4]));
                    hispanicChildrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[5]));
                    hispanicChildren12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[6]));
                }
            } catch (NumberFormatException nfe) {
                //continue
            }
        }
    }

    private void addDataToSeries() {
        data.addSeries(ruralPopulation);
        data.addSeries(childrenUnder12);
        data.addSeries(children12To17);
        hispanicData.addSeries(ruralPopulation);
        hispanicData.addSeries(hispanicChildrenUnder12);
        hispanicData.addSeries(hispanicChildren12To17);
    }

    private void createPlot(XYSeriesCollection data, String title, ApplicationFrame currentFrame) {

        JFreeChart chart = ChartFactory.createScatterPlot(title,
                "State", "Percentage", data,
                PlotOrientation.VERTICAL, true, true, false);

        XYPlot plot = (XYPlot) chart.getPlot();

        SymbolAxis xAxis = new SymbolAxis("State", stateStrings.getStates());
        xAxis.setTickUnit(new NumberTickUnit(1));
        xAxis.setRange(0, stateStrings.getStates().length - 1);
        plot.setDomainAxis(xAxis);

        ChartPanel nonHispanicChartPanel = new ChartPanel(chart);
        nonHispanicChartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
        currentFrame.setContentPane(nonHispanicChartPanel);
        currentFrame.pack();
        RefineryUtilities.centerFrameOnScreen(currentFrame);
        currentFrame.setVisible(true);
    }

    public static void main(String[] args) throws IOException {
        QuestionNinePlot questionNinePlot = new QuestionNinePlot(args[0]);
        questionNinePlot.checkForNA();
        questionNinePlot.plotOutput();
    }
}
