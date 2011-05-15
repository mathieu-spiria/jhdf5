/*
 * Copyright 2011 ETH Zuerich, CISD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.systemsx.cisd.hdf5.examples;

import java.util.Date;

import ch.systemsx.cisd.hdf5.CompoundElement;
import ch.systemsx.cisd.hdf5.HDF5CompoundDataMap;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5SimpleReader;
import ch.systemsx.cisd.hdf5.IHDF5SimpleWriter;

/**
 * This is an advanced example which demonstrates reading and writing a "compound" data set that
 * stores a simple measurement point.
 * 
 * @author Bernd Rinn
 */
public class VoltageMeasurementCompound
{

    /**
     * A Data Transfer Object for a combined temperature / voltage measurement.
     */
    static class Measurement
    {
        private Date date;

        // Include the unit in the member name
        @CompoundElement(memberName = "temperatureInDegreeCelsius")
        private float temperature;

        // Include the unit in the member name
        @CompoundElement(memberName = "voltageInMilliVolts")
        private double voltage;

        // Important: needs to have a default constructor, otherwise JHDF5 will bail out on reading.
        Measurement()
        {
        }

        Measurement(Date date, float temperatureInDegreeCelsius, double voltageInMilliVolts)
        {
            this.date = date;
            this.temperature = temperatureInDegreeCelsius;
            this.voltage = voltageInMilliVolts;
        }

        Date getDate()
        {
            return date;
        }

        void setDate(Date date)
        {
            this.date = date;
        }

        float getTemperature()
        {
            return temperature;
        }

        void setTemperature(float temperatureInDegreeCelsius)
        {
            this.temperature = temperatureInDegreeCelsius;
        }

        double getVoltage()
        {
            return voltage;
        }

        void setVoltage(double voltageInMilliVolts)
        {
            this.voltage = voltageInMilliVolts;
        }

        @Override
        public String toString()
        {
            return "Measurement [date=" + date + ", temperature=" + temperature + ", voltage="
                    + voltage + "]";
        }

    }

    private final IHDF5SimpleWriter hdf5Writer;

    private final IHDF5SimpleReader hdf5Reader;

    VoltageMeasurementCompound(String fileName, boolean readOnly)
    {
        if (readOnly)
        {
            this.hdf5Reader = HDF5Factory.openForReading(fileName);
            this.hdf5Writer = null;
        } else
        {
            this.hdf5Writer = HDF5Factory.open(fileName);
            this.hdf5Reader = hdf5Writer;
        }
    }

    void close()
    {
        hdf5Reader.close();
    }

    void writeMeasurement(String datasetName, Date measurementDate, float measuredTemperature,
            double measuredVoltage)
    {
        hdf5Writer.writeCompound(datasetName, new Measurement(measurementDate, measuredTemperature,
                measuredVoltage));
    }

    Measurement readMeasurement(String datasetName)
    {
        return hdf5Reader.readCompound(datasetName, Measurement.class);
    }

    HDF5CompoundDataMap readMeasurementToMap(String datasetName)
    {
        return hdf5Reader.readCompound(datasetName, HDF5CompoundDataMap.class);
    }

    public static void main(String[] args)
    {
        // Open "voltageMeasurement.h5" for writing
        VoltageMeasurementCompound e =
                new VoltageMeasurementCompound("voltageMeasurement.h5", false);
        e.writeMeasurement("/exeriment3/measurement1", new Date(), 18.6f, 15.38937516);
        System.out.println("Compound record: " + e.readMeasurement("/exeriment3/measurement1"));
        e.close();

        // Open "voltageMeasurement.h5" for reading
        VoltageMeasurementCompound eRO =
                new VoltageMeasurementCompound("voltageMeasurement.h5", true);
        // Note that we have used CompoundElement to use a different member name for the fields
        // "temperature" and "voltage"
        System.out.println("Compound record as map: "
                + eRO.readMeasurementToMap("/exeriment3/measurement1"));
        eRO.close();
    }

}
