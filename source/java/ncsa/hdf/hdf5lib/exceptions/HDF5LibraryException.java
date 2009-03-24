/****************************************************************************
 * NCSA HDF5                                                                 *
 * National Comptational Science Alliance                                   *
 * University of Illinois at Urbana-Champaign                               *
 * 605 E. Springfield, Champaign IL 61820                                   *
 *                                                                          *
 * For conditions of distribution and use, see the accompanying             *
 * hdf-java/COPYING file.                                                  *
 *                                                                          *
 ****************************************************************************/

package ncsa.hdf.hdf5lib.exceptions;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import ncsa.hdf.hdf5lib.H5;

/**
 * <p>
 * The class HDF5LibraryException returns errors raised by the HDF5 library.
 * <p>
 * Each major error code from the HDF-5 Library is represented by a sub-class of this class, and by
 * default the 'detailedMessage' is set according to the minor error code from the HDF-5 Library.
 * <p>
 * For major and minor error codes, see <b>H5Epublic.h</b> in the HDF-5 library.
 * <p>
 */

public class HDF5LibraryException extends HDF5Exception
{
    private static final int UNKNOWN = -1;

    private static final long serialVersionUID = 1L;

    private final int majorErrorNumber;
    
    private final int minorErrorNumber;
    
    private final String majorErrorMessage;

    private final String minorErrorMessage;
    
    private final String hdf5ErrorStackString;

    /**
     * Constructs an <code>HDF5LibraryException</code> with the specified detail message.
     * 
     * @param majorErrorNumber The major error number of the HDF5 library.
     * @param majorErrorMessage The error message for the major error number of the HDF5 library.
     * @param minorErrorNumber The minor error number of the HDF5 library.
     * @param minorErrorMessage The error message for the minor error number of the HDF5 library.
     */
    public HDF5LibraryException(final int majorErrorNumber, final String majorErrorMessage,
            final int minorErrorNumber, final String minorErrorMessage)
    {
        super(majorErrorMessage + ":" + minorErrorMessage);
        // this code forces the loading of the HDF-5 library
        // to assure that the native methods are available
        try
        {
            H5.H5open();
        } catch (final Exception e)
        {
        }
        this.majorErrorNumber = majorErrorNumber;
        this.majorErrorMessage = majorErrorMessage;
        this.minorErrorNumber = minorErrorNumber;
        this.minorErrorMessage = minorErrorMessage;
        this.hdf5ErrorStackString = retrieveHDF5ErrorStackAsString();
    }

    /**
     * Constructs an <code>HDF5LibraryException</code> with the specified detail message.
     * 
     * @param errorMessage The error message for the minor error number of the HDF5 library.
     */
    public HDF5LibraryException(final String errorMessage)
    {
        super(errorMessage);
        // this code forces the loading of the HDF-5 library
        // to assure that the native methods are available
        try
        {
            H5.H5open();
        } catch (final Exception e)
        {
        }
        this.majorErrorNumber = UNKNOWN;
        this.majorErrorMessage = errorMessage;
        this.minorErrorNumber = UNKNOWN;
        this.minorErrorMessage = "";
        this.hdf5ErrorStackString = "No error stack";
    }

    /**
     * Get the major error number of the first error on the HDF5 library error stack.
     * 
     * @return the major error number
     */
    public int getMajorErrorNumber()
    {
        return majorErrorNumber;
    }

    /**
     * Return a error message for the major error number of this exception.
     * <p>
     * These messages come from <b>H5Epublic.h</b>.
     * 
     * @return the string of the minor error
     */
    public String getMajorError()
    {
        return majorErrorMessage;
    }

    /**
     * Get the minor error number of the first error on the HDF5 library error stack.
     * 
     * @return the minor error number
     */
    public int getMinorErrorNumber()
    {
        return minorErrorNumber;
    }

    /**
     * Return a error message for the minor error number of this exception.
     * <p>
     * These messages come from <b>H5Epublic.h</b>.
     * 
     * @return the string of the minor error
     */
    public String getMinorError()
    {
        return minorErrorMessage;
    }

    /**
     * Returns the error stack as retrieved from the HDF5 library as a string.
     */
    private String retrieveHDF5ErrorStackAsString()
    {
        try
        {
            final File tempFile = File.createTempFile("HDF5_error_stack", ".txt");
            try
            {
                printStackTrace0(tempFile.getPath());
                return FileUtils.readFileToString(tempFile).trim();
            } finally
            {
                tempFile.delete();
            }
        } catch (IOException ex)
        {
            System.err.println("Cannot create error stack file.");
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * Returns the error stack from the HDF5 library as a string.
     */
    public String getHDF5ErrorStackAsString()
    {
        return hdf5ErrorStackString;
    }

    /**
     * Prints this <code>HDF5LibraryException</code>, the HDF-5 Library error stack, and and the
     * Java stack trace to the standard error stream.
     */
    @Override
    public void printStackTrace()
    {
        System.err.println(getHDF5ErrorStackAsString()); // the HDF-5 Library error stack
        super.printStackTrace(); // the Java stack trace
    }

    /**
     * Prints this <code>HDF5LibraryException</code> the HDF-5 Library error stack, and and the Java
     * stack trace to the specified print stream.
     */
    public void printStackTrace(final java.io.File f)
    {
        if ((f == null) || !f.exists() || f.isDirectory() || !f.canWrite())
        {
            printStackTrace();
        } else
        {
            try
            {
                final java.io.FileOutputStream o = new java.io.FileOutputStream(f);
                final java.io.PrintWriter p = new java.io.PrintWriter(o);
                p.println(getHDF5ErrorStackAsString()); // the HDF-5 Library error stack
                super.printStackTrace(p); // the Java stack trace
                p.close();
            } catch (final Exception ex)
            {
                System.err.println(this);
            }
        }
    }

    /*
     * This private method calls the HDF-5 library to extract the error codes and error stack.
     */
    private native void printStackTrace0(String s);

}
