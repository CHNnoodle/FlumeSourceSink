package com.xjgreat;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience.Private;
import org.apache.flume.annotations.InterfaceStability.Evolving;
import org.apache.flume.annotations.InterfaceStability.Unstable;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.tools.PlatformDetect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Evolving
public class ReliableSpoolingFileEventPathReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(ReliableSpoolingFileEventPathReader.class);
    static final String metaFileName = ".flumespool-main.meta";
    private final File spoolDirectory;
    private final String completedSuffix;
    private final String deserializerType;
    private final Context deserializerContext;
    private final Pattern ignorePattern;
    private final File metaFile;
    //add by wg
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final boolean annotatePathName;
    private final String fileNameHeader;
    private final String baseNameHeader;
    private final String basePathHeader;
    private final Integer basePathStart;
    private final Integer basePathEnd;


    private final String deletePolicy;
    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;
    private final SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder consumeOrder;
    private Optional<ReliableSpoolingFileEventPathReader.FileInfo> currentFile;
    private Optional<ReliableSpoolingFileEventPathReader.FileInfo> lastFileRead;
    private boolean committed;

    //edit params by wg
    private ReliableSpoolingFileEventPathReader(File spoolDirectory, String completedSuffix, String ignorePattern, String trackerDirPath, boolean annotateFileName, String fileNameHeader, boolean annotateBaseName, String baseNameHeader, boolean annotatePathName, String basePathHeader, Integer basePathStart, Integer basePathEnd, String deserializerType, Context deserializerContext, String deletePolicy, String inputCharset, DecodeErrorPolicy decodeErrorPolicy, SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder consumeOrder) throws IOException {
        this.currentFile = Optional.absent();
        this.lastFileRead = Optional.absent();
        this.committed = true;
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(deletePolicy);
        Preconditions.checkNotNull(inputCharset);
        if(!deletePolicy.equalsIgnoreCase(ReliableSpoolingFileEventPathReader.DeletePolicy.NEVER.name()) && !deletePolicy.equalsIgnoreCase(ReliableSpoolingFileEventPathReader.DeletePolicy.IMMEDIATE.name())) {
            throw new IllegalArgumentException("Delete policies other than NEVER and IMMEDIATE are not yet supported");
        } else {
            if(logger.isDebugEnabled()) {
                logger.debug("Initializing {} with directory={}, metaDir={}, deserializer={}", new Object[]{ReliableSpoolingFileEventPathReader.class.getSimpleName(), spoolDirectory, trackerDirPath, deserializerType});
            }

            Preconditions.checkState(spoolDirectory.exists(), "Directory does not exist: " + spoolDirectory.getAbsolutePath());
            Preconditions.checkState(spoolDirectory.isDirectory(), "Path is not a directory: " + spoolDirectory.getAbsolutePath());

            File trackerDirectory;
            try {
                trackerDirectory = File.createTempFile("flume-spooldir-perm-check-", ".canary", spoolDirectory);
                Files.write("testing flume file permissions\n", trackerDirectory, Charsets.UTF_8);
                List lines = Files.readLines(trackerDirectory, Charsets.UTF_8);
                Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", new Object[]{trackerDirectory});
                if(!trackerDirectory.delete()) {
                    throw new IOException("Unable to delete canary file " + trackerDirectory);
                }

                logger.debug("Successfully created and deleted canary file: {}", trackerDirectory);
            } catch (IOException var17) {
                throw new FlumeException("Unable to read and modify files in the spooling directory: " + spoolDirectory, var17);
            }

            this.spoolDirectory = spoolDirectory;
            this.completedSuffix = completedSuffix;
            this.deserializerType = deserializerType;
            this.deserializerContext = deserializerContext;
            this.annotateFileName = annotateFileName;
            this.fileNameHeader = fileNameHeader;
            this.annotateBaseName = annotateBaseName;
            this.baseNameHeader = baseNameHeader;
            //add by wg
            this.annotatePathName = annotatePathName;
            this.basePathHeader = basePathHeader;
            this.basePathStart = basePathStart;
            this.basePathEnd = basePathEnd;

            this.ignorePattern = Pattern.compile(ignorePattern);
            this.deletePolicy = deletePolicy;
            this.inputCharset = Charset.forName(inputCharset);
            this.decodeErrorPolicy = (DecodeErrorPolicy)Preconditions.checkNotNull(decodeErrorPolicy);
            this.consumeOrder = (SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder)Preconditions.checkNotNull(consumeOrder);
            trackerDirectory = new File(trackerDirPath);
            if(!trackerDirectory.isAbsolute()) {
                trackerDirectory = new File(spoolDirectory, trackerDirPath);
            }

            if(!trackerDirectory.exists() && !trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " + trackerDirectory);
            } else if(!trackerDirectory.isDirectory()) {
                throw new IOException("Specified meta directory is not a directory" + trackerDirectory);
            } else {
                this.metaFile = new File(trackerDirectory, ".flumespool-main.meta");
            }
        }
    }

    public String getLastFileRead() {
        return !this.lastFileRead.isPresent()?null:((ReliableSpoolingFileEventPathReader.FileInfo)this.lastFileRead.get()).getFile().getAbsolutePath();
    }

    public Event readEvent() throws IOException {
        List events = this.readEvents(1);
        return !events.isEmpty()?(Event)events.get(0):null;
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        if(!this.committed) {
            if(!this.currentFile.isPresent()) {
                throw new IllegalStateException("File should not roll when commit is outstanding.");
            }

            logger.info("Last read was never committed - resetting mark position.");
            ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer().reset();
        } else {
            if(!this.currentFile.isPresent()) {
                this.currentFile = this.getNextFile();
            }

            if(!this.currentFile.isPresent()) {
                return Collections.emptyList();
            }
        }

        EventDeserializer des = ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer();
        List events = des.readEvents(numEvents);
        if(events.isEmpty()) {
            this.retireCurrentFile();
            this.currentFile = this.getNextFile();
            if(!this.currentFile.isPresent()) {
                return Collections.emptyList();
            }

            events = ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer().readEvents(numEvents);
        }

        String basename;
        Iterator i$;
        Event event;
        if(this.annotateFileName) {
            basename = ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getFile().getAbsolutePath();
            i$ = events.iterator();

            while(i$.hasNext()) {
                event = (Event)i$.next();
                event.getHeaders().put(this.fileNameHeader, basename);
            }
        }

        if(this.annotateBaseName) {
            basename = ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getFile().getName();
            i$ = events.iterator();

            while(i$.hasNext()) {
                event = (Event)i$.next();
                event.getHeaders().put(this.baseNameHeader, basename);
            }
        }

        //add method for putHeader by wg
        if(this.annotatePathName) {
            String pathname;
            basename = ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getFile().getName();
//            pathname = basename.substring(basePathStart,basePathEnd);

            if(this.basePathStart < this.basePathEnd && basename.length()>=this.basePathEnd ) {
                pathname = basename.substring(basePathStart,basePathEnd);
            }else {
                pathname = basename.substring(0,1);
            }

            i$ = events.iterator();
            while(i$.hasNext()) {
                event = (Event)i$.next();
                event.getHeaders().put(this.basePathHeader, pathname);
            }
        }

        this.committed = false;
        this.lastFileRead = this.currentFile;
        return events;
    }

    public void close() throws IOException {
        if(this.currentFile.isPresent()) {
            ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer().close();
            this.currentFile = Optional.absent();
        }

    }

    public void commit() throws IOException {
        if(!this.committed && this.currentFile.isPresent()) {
            ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer().mark();
            this.committed = true;
        }

    }

    private void retireCurrentFile() throws IOException {
        Preconditions.checkState(this.currentFile.isPresent());
        File fileToRoll = new File(((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getFile().getAbsolutePath());
        ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getDeserializer().close();
        String message;
        if(fileToRoll.lastModified() != ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getLastModified()) {
            message = "File has been modified since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        } else if(fileToRoll.length() != ((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getLength()) {
            message = "File has changed size since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        } else {
            if(this.deletePolicy.equalsIgnoreCase(ReliableSpoolingFileEventPathReader.DeletePolicy.NEVER.name())) {
                this.rollCurrentFile(fileToRoll);
            } else {
                if(!this.deletePolicy.equalsIgnoreCase(ReliableSpoolingFileEventPathReader.DeletePolicy.IMMEDIATE.name())) {
                    throw new IllegalArgumentException("Unsupported delete policy: " + this.deletePolicy);
                }

                this.deleteCurrentFile(fileToRoll);
            }

        }
    }

    private void rollCurrentFile(File fileToRoll) throws IOException {
        File dest = new File(fileToRoll.getPath() + this.completedSuffix);
        logger.info("Preparing to move file {} to {}", fileToRoll, dest);
        boolean renamed;
        String renamed1;
        if(dest.exists() && PlatformDetect.isWindows()) {
            if(!Files.equal(((ReliableSpoolingFileEventPathReader.FileInfo)this.currentFile.get()).getFile(), dest)) {
                renamed1 = "File name has been re-used with different files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(renamed1);
            }

            logger.warn("Completed file " + dest + " already exists, but files match, so continuing.");
            renamed = fileToRoll.delete();
            if(!renamed) {
                logger.error("Unable to delete file " + fileToRoll.getAbsolutePath() + ". It will likely be ingested another time.");
            }
        } else {
            if(dest.exists()) {
                renamed1 = "File name has been re-used with different files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(renamed1);
            }

            renamed = fileToRoll.renameTo(dest);
            if(!renamed) {
                String message = "Unable to move " + fileToRoll + " to " + dest + ". This will likely cause duplicate events. Please verify that " + "flume has sufficient permissions to perform these operations.";
                throw new FlumeException(message);
            }

            logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);
            this.deleteMetaFile();
        }

    }

    private void deleteCurrentFile(File fileToDelete) throws IOException {
        logger.info("Preparing to delete file {}", fileToDelete);
        if(!fileToDelete.exists()) {
            logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
        } else if(!fileToDelete.delete()) {
            throw new IOException("Unable to delete spool file: " + fileToDelete);
        } else {
            this.deleteMetaFile();
        }
    }

    private Optional<ReliableSpoolingFileEventPathReader.FileInfo> getNextFile() {
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                return !candidate.isDirectory() && !fileName.endsWith(ReliableSpoolingFileEventPathReader.this.completedSuffix) && !fileName.startsWith(".") && !ReliableSpoolingFileEventPathReader.this.ignorePattern.matcher(fileName).matches();
            }
        };
        List candidateFiles = Arrays.asList(this.spoolDirectory.listFiles(filter));
        if(candidateFiles.isEmpty()) {
            return Optional.absent();
        } else {
            File selectedFile = (File)candidateFiles.get(0);
            if(this.consumeOrder == SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder.RANDOM) {
                return this.openFile(selectedFile);
            } else {
                Iterator i$;
                File candidateFile;
                long compare;
                if(this.consumeOrder == SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder.YOUNGEST) {
                    i$ = candidateFiles.iterator();

                    while(i$.hasNext()) {
                        candidateFile = (File)i$.next();
                        compare = selectedFile.lastModified() - candidateFile.lastModified();
                        if(compare == 0L) {
                            selectedFile = this.smallerLexicographical(selectedFile, candidateFile);
                        } else if(compare < 0L) {
                            selectedFile = candidateFile;
                        }
                    }
                } else {
                    i$ = candidateFiles.iterator();

                    while(i$.hasNext()) {
                        candidateFile = (File)i$.next();
                        compare = selectedFile.lastModified() - candidateFile.lastModified();
                        if(compare == 0L) {
                            selectedFile = this.smallerLexicographical(selectedFile, candidateFile);
                        } else if(compare > 0L) {
                            selectedFile = candidateFile;
                        }
                    }
                }

                return this.openFile(selectedFile);
            }
        }
    }

    private File smallerLexicographical(File f1, File f2) {
        return f1.getName().compareTo(f2.getName()) < 0?f1:f2;
    }

    private Optional<ReliableSpoolingFileEventPathReader.FileInfo> openFile(File file) {
        try {
            String e = file.getPath();
            DurablePositionTracker tracker = DurablePositionTracker.getInstance(this.metaFile, e);
            if(!tracker.getTarget().equals(e)) {
                tracker.close();
                this.deleteMetaFile();
                tracker = DurablePositionTracker.getInstance(this.metaFile, e);
            }

            Preconditions.checkState(tracker.getTarget().equals(e), "Tracker target %s does not equal expected filename %s", new Object[]{tracker.getTarget(), e});
            ResettableFileInputStream in = new ResettableFileInputStream(file, tracker, 16384, this.inputCharset, this.decodeErrorPolicy);
            EventDeserializer deserializer = EventDeserializerFactory.getInstance(this.deserializerType, this.deserializerContext, in);
            return Optional.of(new ReliableSpoolingFileEventPathReader.FileInfo(file, deserializer));
        } catch (FileNotFoundException var6) {
            logger.warn("Could not find file: " + file, var6);
            return Optional.absent();
        } catch (IOException var7) {
            logger.error("Exception opening file: " + file, var7);
            return Optional.absent();
        }
    }

    private void deleteMetaFile() throws IOException {
        if(this.metaFile.exists() && !this.metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + this.metaFile);
        }
    }

    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix = "fileSuffix";
        private String ignorePattern = "^$";
        private String trackerDirPath = ".flumespool";
        private Boolean annotateFileName = Boolean.valueOf(false);
        private String fileNameHeader = "file";
        private Boolean annotateBaseName = Boolean.valueOf(false);
        private String baseNameHeader = "basename";
        private Boolean annotateBasePathName = Boolean.valueOf(false);
        private String basePathHeader = "basepathname";
        private Integer basePathStart = 0;
        private Integer basePathEnd = 1;

        private String deserializerType = "LINE";
        private Context deserializerContext = new Context();
        private String deletePolicy = "never";
        private String inputCharset = "UTF-8";
        private DecodeErrorPolicy decodeErrorPolicy;
        private SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder consumeOrder;

        public Builder() {
            this.decodeErrorPolicy = DecodeErrorPolicy.valueOf(SpoolDirectoryPathSourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY.toUpperCase());
            this.consumeOrder = SpoolDirectoryPathSourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
        }

        public ReliableSpoolingFileEventPathReader.Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder annotateBasePathName(Boolean annotateBasePathName) {
            this.annotateBasePathName = annotateBasePathName;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder basePathHeader(String basePathHeader) {
            this.basePathHeader = basePathHeader;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder basePathStart(Integer basePathStart) {
            this.basePathStart = basePathStart;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder basePathEnd(Integer basePathEnd) {
            this.basePathEnd = basePathEnd;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public ReliableSpoolingFileEventPathReader.Builder consumeOrder(SpoolDirectoryPathSourceConfigurationConstants.ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public ReliableSpoolingFileEventPathReader build() throws IOException {
            return new ReliableSpoolingFileEventPathReader(this.spoolDirectory, this.completedSuffix, this.ignorePattern, this.trackerDirPath, this.annotateFileName.booleanValue(), this.fileNameHeader, this.annotateBaseName.booleanValue(), this.baseNameHeader,this.annotateBasePathName.booleanValue(), this.basePathHeader, this.basePathStart, this.basePathEnd, this.deserializerType, this.deserializerContext, this.deletePolicy, this.inputCharset, this.decodeErrorPolicy, this.consumeOrder);
        }
    }

    @Private
    @Unstable
    static enum DeletePolicy {
        NEVER,
        IMMEDIATE,
        DELAY;

        private DeletePolicy() {
        }
    }

    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() {
            return this.length;
        }

        public long getLastModified() {
            return this.lastModified;
        }

        public EventDeserializer getDeserializer() {
            return this.deserializer;
        }

        public File getFile() {
            return this.file;
        }
    }
}
