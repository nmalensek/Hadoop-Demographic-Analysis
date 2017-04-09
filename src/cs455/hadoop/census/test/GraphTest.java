package cs455.hadoop.census.test;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class GraphTest extends ApplicationFrame {

    public GraphTest(final String title) {
        super(title);
        final XYSeries series = new XYSeries("Population");
        series.add(0, 453.2);
        series.add(1, 500.2);
        series.add(2, 694.1);
        series.add(3, 100.0);
        series.add(4, 734.4);
        series.add(5, 453.2);
        series.add(6, 500.2);
        series.add(7, 50);
        series.add(8, 734.4);

        final XYSeries urban = new XYSeries("Urban population");
        urban.add(0, 90.2);
        urban.add(1, 200.2);
        urban.add(2, 194.1);
        urban.add(3, 60.0);
        urban.add(4, 334.4);
        urban.add(5, 153.2);
        urban.add(6, 700.2);
        urban.add(7, 5);
        urban.add(8, 333.4);

        final XYSeries random = new XYSeries("random numbers");
        for (int i = 0; i < 50; i++) {
            double randomNum = ThreadLocalRandom.current().nextDouble(100, 700);
            random.add(i, randomNum);
        }
        final XYSeriesCollection data = new XYSeriesCollection();
        data.addSeries(series);
        data.addSeries(urban);
        data.addSeries(random);
        final JFreeChart chart = ChartFactory.createScatterPlot("XY Series", "State", "Y", data,
                PlotOrientation.VERTICAL, true, true, false);

        XYPlot plot = (XYPlot) chart.getPlot();

//        SymbolAxis xAxis = new SymbolAxis("State", states);
//        xAxis.setTickUnit(new NumberTickUnit(1));
//        xAxis.setRange(0, states.length-1);
//        plot.setDomainAxis(xAxis);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 270));
        setContentPane(chartPanel);
    }

//    String[] states = {"CO", "AL", "AK", "DE", "VT", "CT", "AZ", "CA", "SD"};

    public static void main(String[] args) {
        final GraphTest demo = new GraphTest("XY test");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);
    }
}
