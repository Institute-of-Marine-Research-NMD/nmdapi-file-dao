package no.imr.nmdapi.dao.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 *
 * @author kjetilf
 */
public class NMDSeriesReferenceDaoImpl implements NMDSeriesReferenceDao {

    /**
     * Default encoding used.
     */
    public static final String ENCODING = "UTF-8";

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

    private File getFile(String datasetName) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        builder.append(datasetName).append(File.separator);
        builder.append(DATASET_FILENAME);
        return new File(builder.toString());
    }

    public <T> T get(String datasetName) {
        File file = getFile(datasetName);
        if (file.exists()) {
            return unmarshall(file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public <T> void delete(DataTypeEnum type, String datasetName, boolean removeDataset) {
        File file = getFile(datasetName);
        if (!file.exists()) {
            throw new NotFoundException(file.getName().concat(" ").concat(" was not found."));
        }
        file.delete();
        removeDataset(type, datasetName);
        File parentFile = file.getParentFile();
        while (parentFile.list().length == 0) {
            File deleteFile = parentFile;
            parentFile = parentFile.getParentFile();
            FileUtils.deleteQuietly(deleteFile);
        }
    }

    public <T> void update(String datasetName, T data) {
        File file = getFile(datasetName);
        if (file.exists()) {
            marshall(data, file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public <T> void insert(String writeRole, String readRole, String owner, DataTypeEnum type, String datasetName, T data, boolean addDataset) {
        File file = getFile(datasetName);
        if (file.exists()) {
            throw new AlreadyExistsException(file.getName().concat(" already exist."));
        }
        file.getParentFile().mkdirs();
        marshall(data, file);
        addDataset(writeRole, readRole, owner, type, datasetName);
    }

    public boolean hasData(String datasetName) {
        File file = getFile(datasetName);
        return file.exists();
    }

    public List<String> list() {
        List<String> result = new ArrayList<String>();
        File file = getDirectory();
        for (File f : file.listFiles()) {
            if (f.isDirectory()) {
                result.add(f.getName());
            }
        }
        return result;
    }

    public DatasetsType getDatasets() {
        File file = getDatasetFile();
        return unmarshall(file);
    }

    private <T> T unmarshall(final File file) {
        String packages = configuration.getString("app.packages");
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
    }

    private <T> void marshall(final Object data, final File file) {
        String packages = configuration.getString("app.packages");
        try {
            JAXBContext context = JAXBContext.newInstance(packages);
            Marshaller jaxbMarshaller = context.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
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

    private void removeDataset(DataTypeEnum type, String datasetName) {
        File file = getDatasetFile();
        DatasetsType datasets = unmarshall(file);
        if (datasets != null) {
            for (int i = 0; i < datasets.getDataset().size(); i++) {
                DatasetType datasetType = datasets.getDataset().get(i);
                if (datasetType.getDataType() != null && datasetType.getDatasetName() != null && datasetType.getDataType().equals(type) && datasetType.getDatasetName().equalsIgnoreCase(datasetName)) {
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
        } else {
            LOG.error("Did not find dataset ".concat(type.name()).concat(" ").concat(datasetName));
            throw new NotFoundException("Did not find dataset ".concat(type.name()).concat(" ").concat(datasetName));
        }

    }

    private void addDataset(String writeRole, String readRole, String owner, DataTypeEnum type, String datasetName) {
        DatasetType datasetType = new DatasetType();
        String id = "no:imr:".concat(type.toString().toLowerCase()).concat(":").concat(java.util.UUID.randomUUID().toString());
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
        datasetType.setDescription("Datasett for ".concat(type.toString().toLowerCase()));
        RestrictionsType restrictions = new RestrictionsType();
        restrictions.setRead(readRole);
        restrictions.setWrite(writeRole);
        datasetType.setRestrictions(restrictions);
        datasetType.setDataType(type);
        datasetType.setQualityAssured(QualityEnum.NONE);
        datasetType.setOwner(owner);
        datasetType.setDatasetName(datasetName);
        File file = getDatasetFile();
        DatasetsType datasetsType;
        if (file.exists()) {
            datasetsType = unmarshall(file);
            datasetsType.getDataset().add(datasetType);
        } else {
            datasetsType = new DatasetsType();
            datasetsType.getDataset().add(datasetType);
        }
        marshall(datasetsType, file);
    }

    private File getDirectory(String... dirs) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        for (String name : dirs) {
            builder.append(name).append(File.separator);
        }
        return new File(builder.toString());
    }

    private File getDatasetFile() {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        builder.append(DATASET_FILENAME);
        return new File(builder.toString());
    }

    public boolean hasWriteAccess(Collection<String> authorities, DataTypeEnum type, String datasetName) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName);
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

    public boolean hasReadAccess(Collection<String> authorities, DataTypeEnum type, String datasetName) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName);
        if (datasetType != null) {
            if (datasetType.getRestrictions() != null && datasetType.getRestrictions().getRead() != null) {
                if ("unrestricted".equals(datasetType.getRestrictions().getRead())) {
                    access = true;
                } else if (authorities.contains(datasetType.getRestrictions().getRead())) {
                    access = true;
                }
            } else {
                access = true;
            }
        } else {
            access = true;
        }
        return access;
    }

    private DatasetType getDatasetByName(DataTypeEnum type, String datasetName) {
        DatasetsType datasetsType = getDatasets();
        for (DatasetType datasetType : datasetsType.getDataset()) {
            if (datasetType.getDataType().equals(type) && datasetType.getDatasetName().equals(datasetName)) {
                return datasetType;
            }
        }
        return null;
    }

    public void updateDataset(final DatasetType dataset) {
        File file = getDatasetFile();
        if (file.exists()) {
            DatasetsType datasets = unmarshall(file);
            Iterator<DatasetType> datasetsIterator = datasets.getDataset().iterator();
            boolean removed = false;
            while (datasetsIterator.hasNext()) {
                DatasetType datasetType = datasetsIterator.next();
                if (datasetType.getId().equals(dataset.getId())) {
                    datasetsIterator.remove();
                    removed = true;
                }
            }
            if (removed) {
                datasets.getDataset().add(dataset);
                marshall(dataset, file);
            } else {
                throw new NotFoundException("Did not find dataset");
            }
        } else {
            throw new NotFoundException("Dataset file not found.");
        }
    }

    /**
     *
     * @param datasetName
     * @return
     */
    public long getLastModified(String datasetName) {
        File file = getFile(datasetName);
        if (file.exists()) {
            return file.lastModified();
        } else {
            throw new NotFoundException("File was not found.");
        }
    }

    /**
     *
     * @param datasetName
     * @return
     */
    public long getChecksum(String datasetName) {
        File file = getFile(datasetName);
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

    /**
     *
     * @param datasetName
     * @return
     */
    public String getRootNamespace(String datasetName) {
        File file = getFile(datasetName);
        if (file.exists()) {
            Object o = unmarshall(file);
            Package packag = Package.getPackage(o.getClass().getPackage().getName());
            XmlSchema schemaDef = packag.getAnnotation(javax.xml.bind.annotation.XmlSchema.class);
            return schemaDef.namespace();
        } else {
            throw new NotFoundException("Data not found.");
        }
    }

    @Override
    public Resource getFileResource(String name, String type, String subname, String filename) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getString(PRE_DATA_DIR)).append(File.separator);
        builder.append(name).append(File.separator);
        builder.append(type).append(File.separator);
        builder.append(subname).append(File.separator);
        builder.append(filename);
        return new FileSystemResource(new File(builder.toString()));
    }
}
