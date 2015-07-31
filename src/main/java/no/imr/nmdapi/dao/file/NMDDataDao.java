package no.imr.nmdapi.dao.file;

import java.io.File;

/**
 * This handles generic marshall and unmarshall objects to the
 * directory structure.
 *
 * @author kjetilf
 */
public interface NMDDataDao {
   /**
     * Return data.
     *
     * @param <T>   Type of class to return.
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @return              Mission object.
     */
    <T> T get(String missiontype, String year, String platform, String delivery, Class<T> clazz);

    /**
     * Delete biotic data for mission.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     */
    <T> void delete(String missiontype, String year, String platform, String delivery);

    /**
     * Update.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param data
     *
     */
    <T> void update(String missiontype, String year, String platform, String delivery, T data, Class<T> clazz);

    /**
     * Insert data.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param data   data.
     */
    <T> void insert(String missiontype, String year, String platform, String delivery, T data, Class<T> clazz);


    /**
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @return  True if data exists.
     */
    boolean hasData(String missiontype, String year, String platform, String delivery);
}
