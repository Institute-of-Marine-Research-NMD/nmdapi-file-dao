
package no.imr.nmdapi.dao.file;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This is just a generic class for testing inserts updates and deletes.
 *
 * @author kjetilf
 */
@XmlRootElement(name = "Test")
public class TestType {

    private String data;

    public String getData() {
        return data;
    }
        @XmlElement(name = "data")
    public void setData(String data) {
        this.data = data;
    }

}
