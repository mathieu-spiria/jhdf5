/*
 * Copyright 2008 ETH Zuerich, CISD
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

package ch.systemsx.cisd.hdf5.h5ar;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FilenameUtils;

import ch.systemsx.cisd.args4j.Argument;
import ch.systemsx.cisd.args4j.CmdLineException;
import ch.systemsx.cisd.args4j.CmdLineParser;
import ch.systemsx.cisd.args4j.ExampleMode;
import ch.systemsx.cisd.args4j.Option;
import ch.systemsx.cisd.base.exceptions.IErrorStrategy;
import ch.systemsx.cisd.hdf5.BuildAndEnvironmentInfo;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

/**
 * The main class of the HDF5 based archiver.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiverMain
{

    private static final String FILE_EXTENSION_H5 = ".h5";

    private static final String FILE_EXTENSION_H5AR = ".h5ar";

    private enum Command
    {
        ARCHIVE(new String[]
            { "A", "AR", "ARCHIVE" }, false), CAT(new String[]
            { "C", "CT", "CAT" }, true), EXTRACT(new String[]
            { "E", "EX", "EXTRACT" }, true), DELETE(new String[]
            { "D", "RM", "DELETE", "REMOVE" }, false), LIST(new String[]
            { "L", "LS", "LIST" }, true), VERIFY(new String[]
            { "V", "VF", "VERIFY" }, true), HELP(new String[]
            { "H", "HELP" }, true);

        String[] forms;

        boolean readOnly;

        Command(String[] forms, boolean readOnly)
        {
            this.forms = forms;
            this.readOnly = readOnly;
        }

        boolean isReadOnly()
        {
            return readOnly;
        }

        static Command parse(String commandStr)
        {
            final String commandStrU = commandStr.toUpperCase();
            for (Command cmd : values())
            {
                for (String frm : cmd.forms)
                {
                    if (frm.equals(commandStrU))
                    {
                        return cmd;
                    }
                }
            }
            return HELP;
        }
    }

    private final static IErrorStrategy ERROR_STRATEGY_CONTINUE = new IErrorStrategy()
        {
            @Override
            public void dealWithError(Throwable th) throws ArchiverException
            {
                System.err.println(th.getMessage());
            }

            @Override
            public void warning(String message)
            {
                System.err.println(message);
            }
        };

    @Argument
    private List<String> arguments;

    private Command command;

    private File archiveFile;

    private final boolean initializationOK;

    @Option(name = "i", longName = "include", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to include")
    private List<String> fileWhiteList = new ArrayList<String>();

    @Option(name = "e", longName = "exclude", metaVar = "REGEX", usage = "Regex of files to exclude")
    private List<String> fileBlackList = new ArrayList<String>();

    @Option(name = "I", longName = "include-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to include")
    private List<String> dirWhiteList = new ArrayList<String>();

    @Option(name = "E", longName = "exclude-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to exclude")
    private List<String> dirBlackList = new ArrayList<String>();

    @Option(name = "c", longName = "compress", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to compress")
    private List<String> compressionWhiteList = new ArrayList<String>();

    @Option(name = "nc", longName = "no-compression", metaVar = "REGEX", skipForExample = true, usage = "Regex of files not to compress")
    private List<String> compressionBlackList = new ArrayList<String>();

    @Option(name = "C", longName = "compress-all", usage = "Compress all files")
    private Boolean compressAll = null;

    @Option(name = "r", longName = "root-dir", metaVar = "DIR", usage = "Root directory for archiving / extracting / verifying")
    private File rootOrNull;

    @Option(name = "D", longName = "suppress-directories", usage = "Supress output for directories itself for LIST and VERIFY")
    private boolean suppressDirectoryEntries = false;

    @Option(name = "R", longName = "recursive", usage = "Recursive LIST and VERIFY")
    private boolean recursive = false;

    @Option(name = "v", longName = "verbose", usage = "Verbose output (all operations)")
    private boolean verbose = false;

    @Option(name = "q", longName = "quiet", usage = "Quiet operation (only error output)")
    private boolean quiet = false;

    @Option(name = "n", longName = "numeric", usage = "Use numeric values for mode, uid and gid for LIST and VERIFY")
    private boolean numeric = false;

    @Option(name = "t", longName = "test-checksums", usage = "Test CRC32 checksums of files in archive for LIST")
    private boolean testAgainstChecksums = false;

    @Option(name = "a", longName = "verify-attributes", usage = "Consider file attributes for VERIFY")
    private boolean verifyAttributes = false;

    @Option(name = "m", longName = "check-missing-files", usage = "Check for files present on the filesystem but missing from the archive for VERIFY")
    private boolean checkMissingFile = false;

    @Option(longName = "file-format", skipForExample = true, usage = "Specifies the file format version when creating an archive (N=1 -> HDF51.6 (default), N=2 -> HDF51.8)")
    private int fileFormat = 1;

    @Option(longName = "stop-on-error", skipForExample = true, usage = "Stop on first error and give detailed error report")
    private boolean stopOnError = false;

    @Option(longName = "no-sync", skipForExample = true, usage = "Do not sync to disk before program exits (write mode only)")
    private boolean noSync = false;

    private HDF5Archiver archiver;

    /**
     * The command line parser.
     */
    private final CmdLineParser parser = new CmdLineParser(this);

    private HDF5ArchiverMain(String[] args)
    {
        try
        {
            parser.parseArgument(args);
        } catch (CmdLineException ex)
        {
            System.err.printf("Error when parsing command line: '%s'\n", ex.getMessage());
            printHelp(true);
            initializationOK = false;
            return;
        }
        if (arguments == null || arguments.size() < 2)
        {
            printHelp(true);
            initializationOK = false;
            return;
        }
        command = Command.parse(arguments.get(0));
        if (command == null || command == Command.HELP)
        {
            printHelp(true);
            initializationOK = false;
            return;
        }
        if (arguments.get(1).endsWith(FILE_EXTENSION_H5)
                || arguments.get(1).endsWith(FILE_EXTENSION_H5AR))
        {
            archiveFile = new File(arguments.get(1));
        } else
        {
            archiveFile = new File(arguments.get(1) + FILE_EXTENSION_H5AR);
            if (command.isReadOnly() && archiveFile.exists() == false)
            {
                archiveFile = new File(arguments.get(1) + FILE_EXTENSION_H5);
                if (command.isReadOnly() && archiveFile.exists() == false)
                {
                    archiveFile = new File(arguments.get(1));
                }
            }
        }
        if (command.isReadOnly() && archiveFile.exists() == false)
        {
            System.err.println("Archive '" + archiveFile.getAbsolutePath() + "' does not exist.");
            initializationOK = false;
            return;
        }
        if (quiet && verbose)
        {
            System.err.println("Cannot be quiet and verbose at the same time.");
            initializationOK = false;
            return;
        }
        initializationOK = true;
    }

    @Option(longName = "version", skipForExample = true, usage = "Prints out the version information")
    void printVersion(final boolean exit)
    {
        System.err.println("HDF5 archiver version "
                + BuildAndEnvironmentInfo.INSTANCE.getFullVersion());
        if (exit)
        {
            System.exit(0);
        }
    }

    private boolean helpPrinted = false;

    @Option(longName = "help", skipForExample = true, usage = "Shows this help text")
    void printHelp(final boolean dummy)
    {
        if (helpPrinted)
        {
            return;
        }
        parser.printHelp("h5ar", "[option [...]]",
                "[ARCHIVE <archive_file> <item-to-archive> [...] | "
                        + "CAT <archive_file> <item-to-cat> [...] | "
                        + "EXTRACT <archive_file> [<item-to-unarchive> [...]] | "
                        + "DELETE <archive_file> <item-to-delete> [...] | "
                        + "LIST <archive_file> | VERIFY <archive_file>]", ExampleMode.NONE);
        System.err.println("ARCHIVE: add files on the file system to an archive");
        System.err.println("CAT: extract files from an archive to stdout");
        System.err.println("EXTRACT: extract files from an archive to the file system");
        System.err.println("DELETE: delete files from an archive");
        System.err.println("LIST: list files in an archive");
        System.err
                .println("VERIFY: verify the existence and integrity of files on the file system vs. the content of an archive");
        System.err
                .println("Command aliases: ARCHIVE: A, AR; CAT: C, CT; EXTRACT: E, EX; DELETE: D, REMOVE, RM; LIST: L, LS; VERIFY: V, VF");
        System.err.println("Example: h5ar" + parser.printExample(ExampleMode.ALL)
                + " ARCHIVE archive.h5ar .");
        helpPrinted = true;
    }

    private boolean createArchiver()
    {
        final FileFormat fileFormatEnum =
                (fileFormat == 1) ? FileFormat.STRICTLY_1_6 : FileFormat.STRICTLY_1_8;
        try
        {
            archiver =
                    new HDF5Archiver(archiveFile, command.isReadOnly(), noSync, fileFormatEnum,
                            stopOnError ? IErrorStrategy.DEFAULT_ERROR_STRATEGY
                                    : ERROR_STRATEGY_CONTINUE);
        } catch (HDF5JavaException ex)
        {
            // Problem opening the archive file: non readable / writable
            System.err.println("Error opening archive file: " + ex.getMessage());
            return false;
        } catch (HDF5LibraryException ex)
        {
            // Problem opening the archive file: corrupt file
            System.err.println("Error opening archive file: corrupt file ["
                    + ex.getClass().getSimpleName() + ": " + ex.getMessage() + "]");
            return false;
        }
        return true;
    }

    private ArchivingStrategy createArchivingStrategy()
    {
        final ArchivingStrategy strategy =
                new ArchivingStrategy(compressionBlackList.isEmpty() ? ArchivingStrategy.DEFAULT
                        : ArchivingStrategy.DEFAULT_NO_COMPRESSION);
        if (compressAll != null)
        {
            strategy.compressAll(compressAll);
        }
        for (String pattern : fileWhiteList)
        {
            strategy.addToFileWhiteList(pattern);
        }
        for (String pattern : fileBlackList)
        {
            strategy.addToFileBlackList(pattern);
        }
        for (String pattern : dirWhiteList)
        {
            strategy.addToDirWhiteList(pattern);
        }
        for (String pattern : dirBlackList)
        {
            strategy.addToDirBlackList(pattern);
        }
        for (String pattern : fileWhiteList)
        {
            strategy.addToFileWhiteList(pattern);
        }
        for (String pattern : compressionWhiteList)
        {
            strategy.addToCompressionWhiteList(pattern);
        }
        for (String pattern : compressionBlackList)
        {
            strategy.addToCompressionBlackList(pattern);
        }
        return strategy;
    }

    private File getFSRoot()
    {
        return (rootOrNull == null) ? new File(".") : rootOrNull;
    }

    private static class ListingVisitor implements IArchiveEntryVisitor
    {
        private final boolean verifying;

        private final boolean quiet;

        private final boolean verbose;

        private final boolean numeric;

        private final boolean suppressDirectoryEntries;

        private int checkSumFailures;

        ListingVisitor(boolean verifying, boolean quiet, boolean verbose, boolean numeric)
        {
            this(verifying, quiet, verbose, numeric, false);
        }

        ListingVisitor(boolean verifying, boolean quiet, boolean verbose, boolean numeric,
                boolean suppressDirectoryEntries)
        {
            this.verifying = verifying;
            this.quiet = quiet;
            this.verbose = verbose;
            this.numeric = numeric;
            this.suppressDirectoryEntries = suppressDirectoryEntries;
        }

        @Override
        public void visit(ArchiveEntry entry)
        {
            if (suppressDirectoryEntries && entry.isDirectory())
            {
                return;
            }
            if (verifying)
            {
                final boolean ok = entry.isOK();
                if (quiet == false)
                {
                    System.out.println(entry.describeLink(verbose, numeric, true));
                }
                if (ok == false)
                {
                    System.err.println(entry.getStatus(true));
                    ++checkSumFailures;
                }
            } else
            {
                if (quiet == false)
                {
                    System.out.println(entry.describeLink(verbose, numeric, false));
                }
            }
        }

        boolean isOK()
        {
            if (verifying && checkSumFailures > 0)
            {
                System.err.println(checkSumFailures + " file(s) failed the test.");
                return false;
            } else
            {
                return true;
            }
        }
    }

    boolean run()
    {
        if (initializationOK == false)
        {
            return false;
        }
        try
        {
            switch (command)
            {
                case ARCHIVE:
                {
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to archive.");
                        break;
                    }
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    final ArchivingStrategy strategy = createArchivingStrategy();
                    if (verbose)
                    {
                        System.out.printf("Archiving to file '%s', file system root: '%s'\n",
                                archiveFile, getFSRoot());
                    }
                    if (rootOrNull != null)
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            if (verbose)
                            {
                                System.out.printf("  Adding entry: '%s'\n", arguments.get(i));
                            }
                            archiver.archiveFromFilesystem(rootOrNull, new File(rootOrNull,
                                    arguments.get(i)), strategy,
                                    verbose ? IArchiveEntryVisitor.NONVERBOSE_VISITOR : null);
                        }
                    } else
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            if (verbose)
                            {
                                System.out.printf("  Adding entry: '%s'\n", arguments.get(i));
                            }
                            archiver.archiveFromFilesystem(new File(arguments.get(i)), strategy,
                                    true, verbose ? IArchiveEntryVisitor.NONVERBOSE_VISITOR : null);
                        }
                    }
                    break;
                }
                case CAT:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to cat.");
                        break;
                    } else
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            archiver.extractFile(arguments.get(i), new FileOutputStream(
                                    FileDescriptor.out));
                        }
                    }
                    break;
                }
                case EXTRACT:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    final ArchivingStrategy strategy = createArchivingStrategy();
                    if (verbose)
                    {
                        System.out.printf("Extracting from file '%s', file system root: '%s'\n",
                                archiveFile, getFSRoot());
                    }
                    if (arguments.size() == 2)
                    {
                        if (verbose)
                        {
                            System.out.println("  Extracting entry: '/'");
                        }
                        archiver.extractToFilesystem(getFSRoot(), "/", strategy,
                                verbose ? IArchiveEntryVisitor.DEFAULT_VISITOR : quiet ? null
                                        : IArchiveEntryVisitor.NONVERBOSE_VISITOR);
                    } else
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            if (verbose)
                            {
                                System.out.printf("  Extracting entry: '%s'\n", arguments.get(i));
                            }
                            final String unixPath =
                                    FilenameUtils.separatorsToUnix(arguments.get(i));
                            archiver.extractToFilesystem(getFSRoot(), unixPath, strategy,
                                    verbose ? IArchiveEntryVisitor.DEFAULT_VISITOR : quiet ? null
                                            : IArchiveEntryVisitor.NONVERBOSE_VISITOR);
                        }
                    }
                    break;
                }
                case DELETE:
                {
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to delete.");
                        break;
                    }
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    if (verbose)
                    {
                        System.out.printf("Deleting from file '%s'\n", archiveFile);
                        for (String entry : arguments.subList(2, arguments.size()))
                        {
                            System.out.printf("  Deleting entry: '%s'\n", entry);
                        }
                    }
                    archiver.delete(arguments.subList(2, arguments.size()),
                            verbose ? IArchiveEntryVisitor.NONVERBOSE_VISITOR : null);
                    break;
                }
                case VERIFY:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    if (verbose)
                    {
                        System.out.printf("Verifying file '%s', file system root: '%s'\n",
                                archiveFile, getFSRoot());
                    }
                    final String fileOrDir = (arguments.size() > 2) ? arguments.get(2) : "/";
                    final AtomicBoolean haveMissingFiles = new AtomicBoolean();
                    final IArchiveEntryVisitor missingFileVisitorOrNull = checkMissingFile ? new IArchiveEntryVisitor()
                        {
                            @Override
                            public void visit(ArchiveEntry entry)
                            {
                                System.err.println(entry.describeLink(true, false, false) + " (MISSING IN ARCHIVE)");
                                haveMissingFiles.set(true);
                            }
                        } : null;
                    final ListingVisitor visitor =
                            new ListingVisitor(true, quiet, verbose, numeric);
                    archiver.verifyAgainstFilesystem(fileOrDir, getFSRoot(), visitor,
                            missingFileVisitorOrNull, VerifyParameters.build().recursive(recursive)
                                    .numeric(numeric).verifyAttributes(verifyAttributes).get());
                    return visitor.isOK() && haveMissingFiles.get() == false;
                }
                case LIST:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    if (verbose)
                    {
                        System.out.printf("Listing file '%s'\n", archiveFile);
                    }
                    final String fileOrDir = (arguments.size() > 2) ? arguments.get(2) : "/";
                    final ListingVisitor visitor =
                            new ListingVisitor(testAgainstChecksums, quiet, verbose, numeric,
                                    suppressDirectoryEntries);
                    archiver.list(fileOrDir, visitor, ListParameters.build().recursive(recursive)
                            .readLinkTargets(verbose).testArchive(testAgainstChecksums).get());
                    return visitor.isOK();
                }
                case HELP: // Can't happen any more at this point
                    break;
            }
            return true;
        } finally
        {
            if (archiver != null)
            {
                archiver.close();
            }
        }
    }

    public static void main(String[] args)
    {
        final HDF5ArchiverMain main = new HDF5ArchiverMain(args);
        if (main.run() == false)
        {
            System.exit(1);
        }
    }

}
