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
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.DatasetsType;
import no.imr.nmd.commons.dataset.jaxb.QualityEnum;
import no.imr.nmd.commons.dataset.jaxb.RestrictionsType;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
import no.imr.nmdapi.exceptions.S2DException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
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
     * Pre data dir property name.
     */
    private static final String PRE_DATA_DIR = "pre.data.dir";
    /**
     * Application properties.
     */
    @Autowired
    private Configuration configuration;

    private File getFile(String type, String datasetName, String... dirs) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        for (String name : dirs) {
            builder.append(name).append(File.separator);
        }
        builder.append(type).append(File.separator);
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

    public <T> T get(String type, String datasetName, String packageName, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            return unmarshall(packageName, file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public <T> T getByCruisenr(String type, String datasetName, String packageName, String cruisenr) {
        File file = getFileByCruisenr(type, datasetName, cruisenr);
        if (file.exists()) {
            return unmarshall(packageName, file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public boolean hasDataByCruisenr(String type, String datasetName, String packageName, String cruisenr) {
        File file = getFileByCruisenr(type, datasetName, cruisenr);
        return file.exists();
    }

    private <T> T unmarshall(String packageName, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(packageName);
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
    }

    private <T> void marshall(final String packageName, final Object data, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(packageName);
            Marshaller jaxbMarshaller = context.createMarshaller();
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

    public <T> void delete(String type, String datasetName, boolean removeDataset, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (!file.exists()) {
            throw new NotFoundException(file.getName().concat(" ").concat(" was not found."));
        }
        file.delete();
        removeDataset(type, datasetName, dirs);
        File parentFile = file.getParentFile();
        while (parentFile.list().length == 0) {
            File deleteFile = parentFile;
            parentFile = parentFile.getParentFile();
            FileUtils.deleteQuietly(deleteFile);
        }
    }

    public <T> void update(String type, String datasetName, T data, String... names) {
        File file = getFile(type, datasetName, names);
        marshall(data.getClass().getPackage().getName(), data, file);
    }

    public <T> void insert(String writeRole, String readRole, String owner, String type, String datasetName, T data, boolean addDataset, String... dirs) {
        File file = getFile(type, datasetName, dirs);
        if (file.exists()) {
            throw new AlreadyExistsException(file.getName().concat(" already exist."));
        }
        file.getParentFile().mkdirs();
        marshall(data.getClass().getPackage().getName(), data, file);
        addDataset(writeRole, readRole, owner, type, datasetName, dirs);
    }

    public boolean hasData(String type, String datasetName, String... names) {
        File file = getFile(type, datasetName, names);
        return file.exists();
    }

    public List<String> list(String... dirs) {
        List<String> result = new ArrayList<String>();
        File file = getDirectory(dirs);
        result.addAll(Arrays.asList(file.list()));
        return result;
    }

    private void addDataset(String writeRole, String readRole, String owner, String type, String datasetName, String... dirs) {
        DatasetType datasetType = new DatasetType();
        String id = "no:imr:".concat(type.toLowerCase()).concat(":").concat(type.toLowerCase()).concat(java.util.UUID.randomUUID().toString());
        datasetType.setId(id);
        XMLGregorianCalendar cal;
        try {
            cal = DatatypeFactory.newInstance().newXMLGregorianCalendar((GregorianCalendar) GregorianCalendar.getInstance());
            datasetType.setCreated(cal);
            datasetType.setUpdated(cal);
        } catch (DatatypeConfigurationException ex) {
            LOG.error("Error setting created time. ", ex);
            throw new S2DException("Error creating datatype.", ex);
        }
        datasetType.setDescription("Datasett for ".concat(type));
        RestrictionsType restrictions = new RestrictionsType();
        restrictions.setRead(readRole);
        restrictions.setWrite(writeRole);
        datasetType.setRestrictions(restrictions);
        datasetType.setDataType(type.toLowerCase());
        datasetType.setQualityAssured(QualityEnum.NONE);
        datasetType.setOwner(owner);
        datasetType.setDatasetName(datasetName);
        File file = getDatasetFile(dirs);
        DatasetsType datasetsType;
        if (file.exists()) {
            datasetsType = unmarshall(DatasetsType.class.getPackage().getName(), file);
            datasetsType.getDataset().add(datasetType);
        } else {
            datasetsType = new DatasetsType();
            datasetsType.getDataset().add(datasetType);
        }
        marshall(DatasetsType.class.getPackage().getName(), datasetsType, file);
    }

    private void removeDataset(String type, String datasetName, String... dirs) {
        File file = getDatasetFile(dirs);
        DatasetsType datasets = unmarshall(DatasetsType.class.getPackage().getName(), file);
        for (int i = 0; i < datasets.getDataset().size(); i++) {
            DatasetType datasetType = datasets.getDataset().get(i);
            if (datasetType.getDataType().equalsIgnoreCase(type) && datasetType.getDatasetName().equalsIgnoreCase(datasetName)) {
                datasets.getDataset().remove(i);
            }
        }
        if (datasets.getDataset().size() > 0) {
            // Marshall updated dataset file.
            marshall(datasets.getClass().getPackage().getName(), datasets, file);
        } else {
            // remove fe if no datasets exist.
            file.delete();
        }
    }

    public DatasetsType getDatasetsByType(String type, String... dirs) {
        File file = getDatasetFile(dirs);
        DatasetsType datasetsType = unmarshall(DatasetsType.class.getPackage().getName(), file);
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
        return unmarshall(DatasetsType.class.getPackage().getName(), file);
    }

    public boolean hasWriteAccess(Collection<String> authorities, String type, String datasetName, String... dirs) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName, dirs);
        if (datasetType.getRestrictions() != null && datasetType.getRestrictions().getWrite() != null) {
            if (datasetType.getRestrictions().getWrite().equals("unrestricted")) {
                access = true;
            } else if (authorities.contains(datasetType.getRestrictions().getWrite())) {
                access = true;
            }
        } else {
            access = true;
        }
        return access;
    }

    public boolean hasReadAccess(Collection<String> authorities, String type, String datasetName, String... dirs) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName, dirs);
        if (datasetType.getRestrictions() != null && datasetType.getRestrictions().getRead()!= null) {
            if (datasetType.getRestrictions().getRead().equals("unrestricted")) {
                access = true;
            } else if (authorities.contains(datasetType.getRestrictions().getRead())) {
                access = true;
            }
        } else {
            access = true;
        }
        return access;
    }

    private DatasetType getDatasetByName(String type, String datasetName, String... dirs) {
        DatasetsType datasetsType = getDatasetsByType(type, dirs);
        for (DatasetType datasetType : datasetsType.getDataset()) {
            if (datasetType.getDataType().equals(type) && datasetType.getDatasetName().equals(datasetName)) {
                return datasetType;
            }
        }
        return null;
    }

    private File getFileByCruisenr(String type, String datasetName, String cruisenr) {
        String predir = configuration.getString("pre.data.dir");
        Finder finder = new Finder(cruisenr);
        try {
            Files.walkFileTree(Paths.get(predir), finder);
        } catch (IOException ex) {
            LOG.error("Error finding cruisenr", ex);
            throw new S2DException("Error finding cruisenr", ex);
        }
        if (finder.getPath() != null) {
            File file = new File(finder.getPath().toString().concat(File.separator).concat(type).concat(File.separator).concat(datasetName).concat(".xml"));
            LOG.info("get file: " + file.getAbsolutePath());
            return file;
        } else {
            throw new NotFoundException("Cruisenr not found: " + cruisenr);
        }
    }



    public static class Finder
            extends SimpleFileVisitor<Path> {

        private final String name;

        private Path path;

        public Finder(String name) {
            this.name = name;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            String regex = "[0-9]{4}";
            if (dir.getFileName().toString().matches(regex)) {
                if (name.substring(0, 4).equals(dir.getFileName().toString())) {
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }
            } else {
                if (dir.getFileName().toString().equals(name)) {
                    this.path = dir;
                    return FileVisitResult.TERMINATE;
                } else {
                    return FileVisitResult.CONTINUE;
                }
            }
        }

        public Path getPath() {
            return path;
        }

    }

}
