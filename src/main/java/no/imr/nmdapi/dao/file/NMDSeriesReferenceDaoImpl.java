package no.imr.nmdapi.dao.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.List;
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
public class NMDSeriesReferenceDaoImpl implements NMDSeriesReferenceDao {

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
        builder.append(datasetName.concat(".xml"));
        return new File(builder.toString());
    }

    public <T> T get(String datasetName, String packageName) {
        File file = getFile(datasetName);
        if (file.exists()) {
            return unmarshall(packageName, file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public <T> void delete(String type, String datasetName, boolean removeDataset) {
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
            marshall(data.getClass().getPackage().getName(), data, file);
        } else {
            throw new NotFoundException("Not found: " + file.getAbsolutePath());
        }
    }

    public <T> void insert(String writeRole, String readRole, String owner, String type, String datasetName, T data, boolean addDataset) {
        File file = getFile(datasetName);
        if (file.exists()) {
            throw new AlreadyExistsException(file.getName().concat(" already exist."));
        }
        file.getParentFile().mkdirs();
        marshall(data.getClass().getPackage().getName(), data, file);
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
        return unmarshall(DatasetsType.class.getPackage().getName(), file);
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

    private void removeDataset(String type, String datasetName) {
        File file = getDatasetFile();
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

    private void addDataset(String writeRole, String readRole, String owner, String type, String datasetName) {
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
        File file = getDatasetFile();
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

    public boolean hasWriteAccess(Collection<String> authorities, String type, String datasetName) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName);
        if (datasetType.getRestrictions().getWrite().equals("unrestricted")) {
            access = true;
        } else if (authorities.contains(datasetType.getRestrictions().getWrite())) {
            access = true;
        }
        return access;
    }

    public boolean hasReadAccess(Collection<String> authorities, String type, String datasetName) {
        boolean access = false;
        DatasetType datasetType = getDatasetByName(type, datasetName);
        if (datasetType.getRestrictions().getRead().equals("unrestricted")) {
            access = true;
        } else if (authorities.contains(datasetType.getRestrictions().getRead())) {
            access = true;
        }
        return access;
    }

    private DatasetType getDatasetByName(String type, String datasetName) {
        DatasetsType datasetsType = getDatasets();
        for (DatasetType datasetType : datasetsType.getDataset()) {
            if (datasetType.getDataType().equals(type) && datasetType.getDatasetName().equals(datasetName)) {
                return datasetType;
            }
        }
        return null;
    }

}
