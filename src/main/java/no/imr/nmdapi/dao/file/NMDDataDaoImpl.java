package no.imr.nmdapi.dao.file;

import java.io.File;
import java.io.IOException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author kjetilf
 */
public class NMDDataDaoImpl implements NMDDataDao {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(NMDDataDaoImpl.class);

    /**
     * File name used to store data.
     */
    private static final String FILENAME = "data.xml";

    /**
     * Common properties.
     */
    @Autowired
    private Configuration configuration;

    @Override
    public <T> T get(final String missiontype, final String year, final String platform, final String delivery, Class<T> clazz) {
        String predir = configuration.getString("pre.data.dir");
        String postdir = configuration.getString("post.data.dir");
        File file = getFile(missiontype, year, platform, delivery, predir, postdir);
        if (file.exists()) {
            return getFile(clazz, file);
        } else {
            throw new NotFoundException(getFile(missiontype, year, platform, delivery, predir, postdir) + " does not exist.");
        }
    }

    @Override
    public void delete(final String missiontype, final String year, final String platform, final String delivery) {
        String predir = configuration.getString("pre.data.dir");
        String postdir = configuration.getString("post.data.dir");
        File file = getFile(missiontype, year, platform, delivery, predir, postdir);
        if (file.exists()) {
            file.delete();
        } else {
            throw new NotFoundException(getFile(missiontype, year, platform, delivery, predir, postdir) + " does not exist.");
        }
    }

    @Override
    public <T> void update(final String missiontype, final String year, final String platform, final String delivery, final T data, Class<T> clazz) {
        String predir = configuration.getString("pre.data.dir");
        String postdir = configuration.getString("post.data.dir");
        File file = getFile(missiontype, year, platform, delivery, predir, postdir);
        file.getParentFile().mkdirs();
        updateFile(clazz, data, file);
    }

    @Override
    public <T> void insert(final String missiontype, final String year, final String platform, final String delivery, final T data, Class<T> clazz) {
        String predir = configuration.getString("pre.data.dir");
        String postdir = configuration.getString("post.data.dir");
        File file = getFile(missiontype, year, platform, delivery, predir, postdir);
        if (file.exists()) {
            throw new AlreadyExistsException(getFile(missiontype, year, platform, delivery, predir, postdir) + " already exist.");
        } else {
            file.getParentFile().mkdirs();
            createFile(clazz, data, file);
        }
    }

    @Override
    public boolean hasData(String missiontype, String year, String platform, String delivery) {
        String predir = configuration.getString("pre.data.dir");
        String postdir = configuration.getString("post.data.dir");
        File file = getFile(missiontype, year, platform, delivery, predir, postdir);
        return file.exists();
    }

    /**
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param predir
     * @param postdir
     * @return
     */
    private File getFile(final String missiontype, final String year, final String platform, final String delivery, final String predir, final String postdir) {
        return new File(predir + System.getProperty("file.separator") + missiontype + System.getProperty("file.separator") + year + System.getProperty("file.separator") + platform + System.getProperty("file.separator") + delivery + System.getProperty("file.separator") + postdir + System.getProperty("file.separator") + FILENAME);
    }

    private <T> T getFile(final Class<T> classes, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(classes.getPackage().getName());
            Unmarshaller jaxbMarshaller = context.createUnmarshaller();
            Object response = jaxbMarshaller.unmarshal(file);
            if (response instanceof JAXBElement) {
                return (T) ((JAXBElement) response).getValue();
            } else {
                return (T) response;
            }
        } catch (JAXBException ex) {
            LOG.error("Error unmarshalling. ", ex);
        }
        return null; // TODO add throws if unmarshall fails.
    }

    private <T> void updateFile(final Class<T> aClass, final Object obj, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(aClass.getPackage().getName());
            Marshaller jaxbMarshaller = context.createMarshaller();
            file.createNewFile();
            jaxbMarshaller.marshal(obj, file);
        } catch (JAXBException ex) {
            LOG.error("Error unmarshalling. ", ex);
        } catch (IOException ex) {
            LOG.error("Error unmarshalling. ", ex);
        }
    }

    private void createFile(final Class<?> aClass, final Object obj, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(aClass.getPackage().getName());
            Marshaller jaxbMarshaller = context.createMarshaller();
            file.createNewFile();
            jaxbMarshaller.marshal(obj, file);
        } catch (JAXBException ex) {
            LOG.error("Error unmarshalling. ", ex);
        } catch (IOException ex) {
            LOG.error("Error unmarshalling. ", ex);
        }
    }

    private Object getFile(final String packageName, final File file) {
        try {
            JAXBContext context = JAXBContext.newInstance(packageName);
            Unmarshaller jaxbMarshaller = context.createUnmarshaller();
            Object response = jaxbMarshaller.unmarshal(file);
            if (response instanceof JAXBElement) {
                return ((JAXBElement) response).getValue();
            } else {
                return response;
            }
        } catch (JAXBException ex) {
            LOG.error("Error unmarshalling. ", ex);
        }
        return null; // TODO add throws if unmarshall fails.
    }

}
