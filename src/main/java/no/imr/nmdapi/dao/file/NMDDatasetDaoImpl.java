package no.imr.nmdapi.dao.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.ListIterator;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlSchema;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import no.imr.nmd.commons.dataset.jaxb.DataTypeEnum;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.DatasetsType;
import no.imr.nmd.commons.dataset.jaxb.QualityEnum;
import no.imr.nmd.commons.dataset.jaxb.RestrictionsType;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
import no.imr.nmdapi.exceptions.S2DException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author kjetilf
 */
public class NMDDatasetDaoImpl implements NMDDatasetDao {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(NMDDatasetDaoImpl.class);

    /**
     * Dataset filename.
     */
    private static final String DATASET_FILENAME = "data.xml";

    /**
     * Pre data dir property cruisenr.
     */
    private static final String PRE_DATA_DIR = "pre.data.dir";
    /**
     * Application properties.
     */
    @Autowired
    private Configuration configuration;

    private File getFile(DataTypeEnum type, String datasetName, String... dirs) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        List<String> strs = new ArrayList<String>();
        strs.addAll(Arrays.asList(dirs));
        strs.add(4, type.name().toLowerCase());
        for (String name : strs) {
            builder.append(name).append(File.separator);
        }
        builder.append(datasetName.concat(".xml"));
        return new File(builder.toString());
    }

    private File getDatasetFile(String... dirs) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        for (String name : dirs) {
            builder.append(name).append(File.separator);
        }
        builder.append(DATASET_FILENAME);
        return new File(builder.toString());
    }

    private File getDirectory(String... dirs) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        for (String name : dirs) {
            builder.append(name).append(File.separator);
        }
        return new File(builder.toString());
    }

    public <T> T get(DataTypeEnum type, String datasetName, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            return unmarshall(file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public String getRootNamespace(DataTypeEnum type, String datasetName, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            Object o = unmarshall(file);
            Package packag = Package.getPackage(o.getClass().getPackage().getName());
            XmlSchema schemaDef = packag.getAnnotation(javax.xml.bind.annotation.XmlSchema.class);
            return schemaDef.namespace();
        } else {
            throw new NotFoundException("Data not found.");
        }
    }

    public Path getByCruisenr(DataTypeEnum type, String datasetName, String cruisenr, String shipname) {
        return getFileByCruisenr(type, cruisenr, shipname);
    }

    public boolean hasDataByCruisenr(DataTypeEnum type, String datasetName, String cruisenr, String shipname) {
        Path path = getFileByCruisenr(type, cruisenr, shipname);
        File file = new File(path.toString().concat(File.separator).concat(type.name().toLowerCase()).concat(File.separator).concat(datasetName).concat(".xml"));
        return file.exists();
    }

    private <T> T unmarshall(final File file) {
        String packages = configuration.getString("app.packages");
        if (file.exists()) {
            try {
                JAXBContext context = JAXBContext.newInstance(packages);
                Unmarshaller jaxbMarshaller = context.createUnmarshaller();
                Object response = jaxbMarshaller.unmarshal(file);
                if (response instanceof JAXBElement) {
                    return (T) ((JAXBElement) response).getValue();
                } else {
                    return (T) response;
                }
            } catch (JAXBException ex) {
                LOG.error("Error unmarshalling. ", ex);
                throw new S2DException("Could not get data.");
            }
        } else {
            return (T)new DatasetsType();
        }
    }

    private <T> void marshall(final Object data, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(data.getClass().getPackage().getName());
            Marshaller jaxbMarshaller = context.createMarshaller();
            file.getParentFile().mkdirs();
            file.createNewFile();
            jaxbMarshaller.marshal(data, file);
        } catch (JAXBException ex) {
            LOG.error("Error unmarshalling. ", ex);
            throw new S2DException("Could not marshall object. ", ex);
        } catch (IOException ex) {
            LOG.error("Error unmarshalling. ", ex);
            throw new S2DException("Could not marshall object. ", ex);
        }
    }

    public <T> void delete(DataTypeEnum type, String datasetName, boolean removeDataset, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (!file.exists()) {
            throw new NotFoundException(file.getName().concat(" ").concat(" was not found."));
        }
        file.delete();
        File parentFile = file.getParentFile();
        while (parentFile.list().length == 0) {
            File deleteFile = parentFile;
            parentFile = parentFile.getParentFile();
            FileUtils.deleteQuietly(deleteFile);
        }
    }

    public <T> void update(DataTypeEnum type, String datasetName, T data, String... names) {
        File file = getFile(type, datasetName, names);
        marshall(data, file);
    }

    public <T> void insert(DataTypeEnum type, String datasetName, T data, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            throw new AlreadyExistsException(file.getName().concat(" already exist."));
        }
        file.getParentFile().mkdirs();
        marshall(data, file);
    }

    public boolean hasData(DataTypeEnum type, String datasetName, String... names) {
        File file = getFile(type, datasetName, names);
        return file.exists();
    }

    public List<String> list(String... dirs) {
        List<String> result = new ArrayList<String>();
        File file = getDirectory(dirs);
        result.addAll(Arrays.asList(file.list()));
        return result;
    }

    public void removeDataset(DataTypeEnum type, String datasetName, String... dirs) {
        File file = getDatasetFile(dirs);
        DatasetsType datasets = unmarshall(file);
        for (int i = 0; i < datasets.getDataset().size(); i++) {
            DatasetType datasetType = datasets.getDataset().get(i);
            if (datasetType.getDataType().equals(type) && datasetType.getDatasetName().equalsIgnoreCase(datasetName)) {
                datasets.getDataset().remove(i);
            }
        }
        if (!datasets.getDataset().isEmpty()) {
            // Marshall updated dataset file.
            marshall(datasets, file);
        } else {
            // remove fe if no datasets exist.
            file.delete();
        }
    }

    public DatasetsType getDatasetsByType(DataTypeEnum type, String... dirs) {
        File file = getDatasetFile(dirs);
        DatasetsType datasetsType = unmarshall(file);
        if (datasetsType == null) {
            return null;
        }
        ListIterator<DatasetType> it = datasetsType.getDataset().listIterator();
        List<DatasetType> datasetsOfType = new ArrayList<DatasetType>();
        while (it.hasNext()) {
            DatasetType datasetType = it.next();
            if (datasetType.getDataType().equals(type)) {
                datasetsOfType.add(datasetType);
            }
        }
        DatasetsType result = new DatasetsType();
        result.getDataset().addAll(datasetsOfType);
        return result;
    }

    public DatasetsType getDatasets(String... dirs) {
        File file = getDatasetFile(dirs);
        return unmarshall(file);
    }

    public boolean hasWriteAccess(Collection<String> authorities, DataTypeEnum type, String datasetName, String... dirs) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName, dirs);
        if (datasetType.getRestrictions() != null && datasetType.getRestrictions().getWrite() != null) {
            if ("unrestricted".equals(datasetType.getRestrictions().getWrite())) {
                access = true;
            } else if (authorities.contains(datasetType.getRestrictions().getWrite())) {
                access = true;
            }
        } else {
            access = true;
        }
        return access;
    }

    public boolean hasReadAccess(Collection<String> authorities, DataTypeEnum type, String datasetName, String... dirs) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName, dirs);
        if (datasetType != null && datasetType.getRestrictions() != null && datasetType.getRestrictions().getRead() != null) {
            if ("unrestricted".equals(datasetType.getRestrictions().getRead())) {
                access = true;
            } else if (authorities.contains(datasetType.getRestrictions().getRead())) {
                access = true;
            }
        } else {
            access = true;
        }
        return access;
    }

    public DatasetType getDatasetByName(DataTypeEnum type, String datasetName, String... dirs) {
        DatasetsType datasetsType = getDatasetsByType(type, dirs);
        for (DatasetType datasetType : datasetsType.getDataset()) {
            if (datasetType.getDataType().equals(type) && datasetType.getDatasetName() != null && datasetType.getDatasetName().equalsIgnoreCase(datasetName)) {
                return datasetType;
            }
        }
        return null;
    }

    private Path getFileByCruisenr(DataTypeEnum type, String cruisenr, String shipname) {
        String predir = configuration.getString("pre.data.dir");
        Finder finder = new Finder(cruisenr, shipname, type);
        try {
            Files.walkFileTree(Paths.get(predir), finder);
        } catch (IOException ex) {
            LOG.error("Error finding cruisenr", ex);
            throw new S2DException("Error finding cruisenr", ex);
        }
        if (finder.getPath() != null) {
            LOG.info("get file: " + finder.getPath().toString());
            return finder.getPath();
        } else {
            throw new NotFoundException("Cruisenr not found: " + cruisenr);
        }
    }

    @Override
    public long getLastModified(DataTypeEnum type, String datasetName, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            return file.lastModified();
        } else {
            throw new NotFoundException("File was not found.");
        }
    }

    @Override
    public long getChecksum(DataTypeEnum type, String datasetName, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            try {
                return FileUtils.checksumCRC32(file);
            } catch (IOException ex) {
                throw new S2DException("Application error occured when generating checksum.", ex);
            }
        } else {
            throw new NotFoundException("File was not found.");
        }
    }

    public void createDataset(String writeRole, String readRole, String description, String owner, QualityEnum quality, DataTypeEnum type, String datasetName, XMLGregorianCalendar cal, String... dirs) {
        if (!hasDataset(type, datasetName, dirs)) {
            DatasetType datasetType = new DatasetType();
            datasetType.setCreated(cal);
            datasetType.setUpdated(cal);
            datasetType.setDescription(description);
            RestrictionsType restrictions = new RestrictionsType();
            restrictions.setRead(readRole);
            restrictions.setWrite(writeRole);
            datasetType.setRestrictions(restrictions);
            datasetType.setDataType(type);
            datasetType.setQualityAssured(quality);
            datasetType.setOwner(owner);
            datasetType.setDatasetName(datasetName);
            String id = "no:imr:".concat(type.toString()).concat(":").concat(java.util.UUID.randomUUID().toString());
            datasetType.setId(id);
            File file = getDatasetFile(dirs);
            DatasetsType datasetsType;

            if (file.exists()) {
                datasetsType = unmarshall(file);
                datasetsType.getDataset().add(datasetType);
            } else {
                datasetsType = new DatasetsType();
                datasetsType.getDataset().add(datasetType);
            }
            marshall(datasetsType, file);
        } else {
            throw new S2DException("Dataset already exists.");
        }
    }

    public void updateDataset(DataTypeEnum type, String datasetName, XMLGregorianCalendar cal, String... dirs) {
        if (hasDataset(type, datasetName, dirs)) {
            File file = getDatasetFile(dirs);
            DatasetsType datasetsType = getDatasets(dirs);
            ListIterator<DatasetType> it = datasetsType.getDataset().listIterator();
            boolean found = false;
            while (it.hasNext()) {
                DatasetType datasetType = it.next();
                if (datasetType.getDataType().equals(type) && datasetType.getDatasetName().equals(datasetName)) {
                    found = true;
                    datasetType.setUpdated(cal);
                }
            }
            if (found) {
                marshall(datasetsType, file);
            } else {
                LOG.error("Dataset could not be updated.".concat(dirs.toString()).concat(" ").concat(type.name()));
                throw new NotFoundException("Dataset could not be updated.".concat(Arrays.toString(dirs)).concat(" ").concat(type.name()));
            }
        } else {
            throw new S2DException("Dataset does not exist.");
        }
    }

    public boolean hasDataset(DataTypeEnum type, String datasetName, String... dirs) {
        if (getDatasetByName(type, datasetName, dirs) != null) {
            return true;
        } else {
            return false;
        }
    }

    public void updateDataset(DatasetType dataset, String... container) {
        File file = getDatasetFile(container);
        DatasetsType datasetsType = getDatasets(container);
        ListIterator<DatasetType> it = datasetsType.getDataset().listIterator();
        boolean found = false;
        while (it.hasNext()) {
            DatasetType testDatasetType = it.next();
            if (dataset.getDataType().equals(testDatasetType.getDataType())
                    && dataset.getDatasetName().equals(testDatasetType.getDatasetName())
                    && dataset.getId().equals(testDatasetType.getId())) {
                try {
                    found = true;
                    GregorianCalendar cal = new GregorianCalendar();
                    dataset.setUpdated(DatatypeFactory.newInstance().newXMLGregorianCalendar(cal));
                    it.remove();
                } catch (DatatypeConfigurationException ex) {
                    LOG.warn("Could not set updated time.", ex);
                }
            }
        }
        if (!found) {
            LOG.info("Could not find dataset.");
            throw new NotFoundException("Could not find dataset.");
        }
        datasetsType.getDataset().add(dataset);
        marshall(datasetsType, file);
    }

    public static class Finder
            extends SimpleFileVisitor<Path> {
        /**
         * Container missiontype.
         */
        private static final int CONTAINER_MISSIONTYPE = 1;
        /**
         *
         */
        private static final int CONTAINER_YEAR = 2;
        /**
         *
         */
        private static final int CONTAINER_PLATFORM = 3;
        /**
         *
         */
        private static final int CONTAINER_DELIVERY = 4;
        /**
         *
         */
        private static final int CONTAINER_DATATYPE = 5;
        /**
         *
         */
        private static final int CRUISENR_TO_YEAR_PRE_INDEX = 0;
        /**
         *
         */
        private static final int CRUISENR_TO_YEAR_POST_INDEX = 4;

        private final String cruisenr;

        private final String shipname;

        private final DataTypeEnum type;

        private Path path;

        private int initPath = -1;

        public Finder(String cruisenr, String shipname, DataTypeEnum type) {
            this.cruisenr = cruisenr;
            this.shipname = shipname;
            this.type = type;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (initPath == -1) {
                initPath = dir.getNameCount();
            }
            String searchShipName = this.shipname.replace(".", " ");
            if ((dir.getNameCount() - initPath) == 0) {
                return FileVisitResult.CONTINUE;
            } else if ((dir.getNameCount() - initPath) == CONTAINER_MISSIONTYPE) {
                return FileVisitResult.CONTINUE;
            } else if ((dir.getNameCount() - initPath) == CONTAINER_YEAR && StringUtils.equals(dir.getName(dir.getNameCount() - 1).toString(), cruisenr.substring(CRUISENR_TO_YEAR_PRE_INDEX, CRUISENR_TO_YEAR_POST_INDEX))) {
                return FileVisitResult.CONTINUE;
            } else if ((dir.getNameCount() - initPath) == CONTAINER_PLATFORM && StringUtils.containsIgnoreCase(dir.getName(dir.getNameCount() - 1).toString(), searchShipName)) {
                return FileVisitResult.CONTINUE;
            } else if ((dir.getNameCount() - initPath) == CONTAINER_DELIVERY && StringUtils.equals(dir.getName(dir.getNameCount() - 1).toString(), cruisenr)) {
                return FileVisitResult.CONTINUE;
            } else if ((dir.getNameCount() - initPath) == CONTAINER_DATATYPE && StringUtils.equalsIgnoreCase(dir.getName(dir.getNameCount() - 1).toString(), type.toString())) {
                this.path = dir;
                return FileVisitResult.TERMINATE;
            } else {
                return FileVisitResult.SKIP_SUBTREE;
            }
        }

        public Path getPath() {
            return path;
        }

    }

}
