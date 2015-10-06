package no.imr.nmdapi.dao.file;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import no.imr.nmd.commons.dataset.jaxb.DataTypeEnum;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.DatasetsType;
import no.imr.nmd.commons.dataset.jaxb.QualityEnum;

/**
 * This handles generic marshall and unmarshall objects to the
 * directory structure.
 *
 * @author kjetilf
 */
public interface NMDDatasetDao {

    /**
     * Get data for a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    <T> T get(DataTypeEnum type, String datasetName, String... dirs);

    /**
     * Delete a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param removeDataset
     * @param dirs
     */
    <T> void delete(DataTypeEnum type, String datasetName, boolean removeDataset, String... dirs);

    /**
     * Update the dataset for a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param dirs
     * @param data
     */
    <T> void update(DataTypeEnum type, String datasetName, T data, String... dirs);

    /**
     * Insert a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param data
     * @param dirs
     */
    <T> void insert(DataTypeEnum type, String datasetName, T data, String... dirs);

    /**
     * Does the data exist.
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasData(DataTypeEnum type, String datasetName, String... dirs);

    /**
     * Returns a list of files for a specific series. It uses pre.data.dir to
     * get pre the directory
     *
     * @param dirs
     * @return  A list of files.
     */
    List<String> list(String... dirs);

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasWriteAccess(Collection<String> authorities, DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasReadAccess(Collection<String> authorities, DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param cruisenr
     * @param shipname
     * @return
     */
    Path getByCruisenr(DataTypeEnum type, String datasetName, String cruisenr, String shipname);

    /**
     *
     * @param type
     * @param datasetName
     * @param cruisenr
     * @param shipname
     * @return
     */
    boolean hasDataByCruisenr(DataTypeEnum type, String datasetName, String cruisenr, String shipname);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    String getRootNamespace(DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    long getLastModified(DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    long getChecksum(DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param writeRole
     * @param readRole
     * @param description
     * @param owner
     * @param quality
     * @param type
     * @param datasetName
     * @param dirs
     * @param cal
     */
    void createDataset(String writeRole, String readRole, String description, String owner, QualityEnum quality, DataTypeEnum type, String datasetName, XMLGregorianCalendar cal, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param cal
     * @param dirs
     */
    void updateDataset(DataTypeEnum type, String datasetName, XMLGregorianCalendar cal, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasDataset(DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     */
    void removeDataset(DataTypeEnum type, String datasetName, String... dirs);

    /**
     *
     * @param dirs
     * @return
     */
    DatasetsType getDatasets(String... dirs);

    /**
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    DatasetType getDatasetByName(DataTypeEnum type, String datasetName, String... dirs);



}
