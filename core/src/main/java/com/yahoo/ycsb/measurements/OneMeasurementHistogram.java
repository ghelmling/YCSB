/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;

import org.HdrHistogram.AtomicHistogram;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement
{
    final static int SIGNIFICANT_DIGITS = 4;

    AtomicHistogram histogram;
    long windowoperations;
    long windowtotallatency;
    long totallatency;
    HashMap<Integer,int[]> returncodes;

    public OneMeasurementHistogram(String name, Properties props)
    {
        super(name);
        // Use Integer.MAX_VALUE as highest trackable value with significance
        // out to 4 decimal places
        histogram = new AtomicHistogram(Integer.MAX_VALUE, SIGNIFICANT_DIGITS);
        returncodes=new HashMap<Integer,int[]>();
    }

    /* (non-Javadoc)
     * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
     */
    public synchronized void reportReturnCode(int code)
    {
        Integer Icode=code;
        if (!returncodes.containsKey(Icode))
        {
            int[] val=new int[1];
            val[0]=0;
            returncodes.put(Icode,val);
        }
        returncodes.get(Icode)[0]++;
    }

    /* (non-Javadoc)
     * @see com.yahoo.ycsb.OneMeasurement#measure(int)
     */
    public synchronized void measure(int latency)
    {
        histogram.recordValue(latency);
        windowoperations++;
        windowtotallatency += latency;
        totallatency += latency;
    }

    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException
    {
        double mean = histogram.getMean();
        long max = histogram.getMaxValue();
        AtomicHistogram corrected = null;

        // This only works if all measures are the same unit
        if (mean < ((max * max) / (2 * totallatency))) {
        // Suspected coordinated omission, compensate any measure higher than
        // mean plus two standard deviations
        // TODO: Better expected measurement threshold
        corrected = histogram.copyCorrectedForCoordinatedOmission((long)
            (histogram.getMean() + (2 * histogram.getStdDeviation())));
        }

        exporter.write(getName(), "Operations", (long)histogram.getTotalCount());
        exporter.write(getName(), "AverageLatency(us)", histogram.getMean());
        if (corrected != null) {
            exporter.write(getName(), "AverageLatency(us,corrected)",
                corrected.getMean());
        }
        exporter.write(getName(), "MinLatency(us)", histogram.getMinValue());
        exporter.write(getName(), "MaxLatency(us)", histogram.getMaxValue());
        if (corrected != null) {
            exporter.write(getName(), "MaxLatency(us,corrected)",
                corrected.getMaxValue());
        }
        exporter.write(getName(), "95thPercentileLatency(ms)",
            histogram.getValueAtPercentile(0.95) / 1000);
        if (corrected != null) {
            exporter.write(getName(), "95thPercentileLatency(ms,corrected)",
                corrected.getValueAtPercentile(0.95) / 1000);
        }
        exporter.write(getName(), "99thPercentileLatency(ms)",
            histogram.getValueAtPercentile(0.99) / 1000);
        if (corrected != null) {
            exporter.write(getName(), "99thPercentileLatency(ms,corrected)",
                corrected.getValueAtPercentile(0.99) / 1000);
        }

        for (Integer I : returncodes.keySet()) {
            int[] val=returncodes.get(I);
            exporter.write(getName(), "Return="+I, val[0]);
        }
    }

    @Override
    public String getSummary() {
        if (windowoperations==0)
        {
            return "";
        }
        DecimalFormat d = new DecimalFormat("#.##");
        double report=((double)windowtotallatency)/((double)windowoperations);
        windowtotallatency=0;
        windowoperations=0;
        return "["+getName()+" AverageLatency(us)="+d.format(report)+"]";
    }

}
