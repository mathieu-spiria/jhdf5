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

public class HDF5ReferenceException extends HDF5LibraryException
{
    private static final long serialVersionUID = 1L;

    /**
     * Constructs an <code>HDF5ReferenceException</code> with the specified detail message.
     * 
     * @param majorErrorNumber The major error number of the HDF5 library.
     * @param majorErrorMessage The error message for the major error number of the HDF5 library.
     * @param minorErrorNumber The minor error number of the HDF5 library.
     * @param minorErrorMessage The error message for the minor error number of the HDF5 library.
     */
    public HDF5ReferenceException(final int majorErrorNumber, final String majorErrorMessage,
            final int minorErrorNumber, final String minorErrorMessage)
    {
        super(majorErrorNumber, majorErrorMessage, minorErrorNumber, minorErrorMessage);
    }
}
