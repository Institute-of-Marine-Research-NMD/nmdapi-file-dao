package no.imr.nmdapi.dao.file;

import java.util.Collection;
import java.util.List;
import no.imr.nmd.commons.dataset.jaxb.DataTypeEnum;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.DatasetsType;

/**
 * This handles generic marshall and unmarshall objects to the
 * directory structure.
 *
 * @author kjetilf
 */
public interface NMDSeriesReferenceDao {

    /**
     * Get data for a single dataset.
     *
     * @param <T>
     * @param datasetName
     * @return
     */
    <T> T get(String datasetName);

    /**
     * Delete a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param removeDataset
     */
    <T> void delete(DataTypeEnum type, String datasetName, boolean removeDataset);

    /**
     * Update the dataset for a single dataset.
     *
     * @param <T>
     * @param datasetName
     * @param data
     */
    <T> void update(String datasetName, T data);

    /**
     * Insert a single dataset.
     *
     * @param <T>
     * @param writeRole
     * @param readRole
     * @param owner
     * @param type
     * @param datasetName
     * @param addDataset
     * @param data
     */
    <T> void insert(String writeRole, String readRole, String owner, DataTypeEnum type, String datasetName, T data, boolean addDataset);

    /**
     * Does the data exist.
     *
     * @param datasetName
     * @return
     */
    boolean hasData(String datasetName);

    /**
     * Returns a list of files for a specific series. It uses pre.data.dir to
     * get pre the directory
     *
     * @return  A list of files.
     */
    List<String> list();

    /**
     *  Get all datasets.
     *
     * @return
     */
    DatasetsType getDatasets();

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @return
     */
    boolean hasWriteAccess(Collection<String> authorities, DataTypeEnum type, String datasetName);

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @return
     */
    boolean hasReadAccess(Collection<String> authorities, DataTypeEnum type, String datasetName);

    /**
     * Update dataset information.
     *
     * @param dataset   information.
     */
    void updateDataset(final DatasetType dataset);

    /**
     *
     * @param datasetName
     * @return
     */
    long getLastModified(String datasetName);

    /**
     *
     * @param datasetName
     * @return
     */
    long getChecksum(String datasetName);
    /**
     *
     * @param datasetName
     * @return
     */
    String getRootNamespace(String datasetName);

}
