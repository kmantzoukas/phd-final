//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.07.24 at 12:58:26 PM BST 
//


package org.slaatsoi.eventschema;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for PredictionResultEventType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PredictionResultEventType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="SecurityPropertyInfo" type="{http://www.slaatsoi.org/eventschema}SecurityPropertyType"/>
 *         &lt;element name="PredictionInfo" type="{http://www.slaatsoi.org/eventschema}PredictionInfoType"/>
 *         &lt;element name="ExtraProperties" type="{http://www.slaatsoi.org/eventschema}PropertiesType"/>
 *       &lt;/sequence>
 *       &lt;attribute name="type" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PredictionResultEventType", propOrder = {
    "securityPropertyInfo",
    "predictionInfo",
    "extraProperties"
})
public class PredictionResultEventType
    implements Serializable
{

    private final static long serialVersionUID = 1L;
    @XmlElement(name = "SecurityPropertyInfo", required = true)
    protected SecurityPropertyType securityPropertyInfo;
    @XmlElement(name = "PredictionInfo", required = true)
    protected PredictionInfoType predictionInfo;
    @XmlElement(name = "ExtraProperties", required = true)
    protected PropertiesType extraProperties;
    @XmlAttribute(required = true)
    protected String type;

    /**
     * Gets the value of the securityPropertyInfo property.
     * 
     * @return
     *     possible object is
     *     {@link SecurityPropertyType }
     *     
     */
    public SecurityPropertyType getSecurityPropertyInfo() {
        return securityPropertyInfo;
    }

    /**
     * Sets the value of the securityPropertyInfo property.
     * 
     * @param value
     *     allowed object is
     *     {@link SecurityPropertyType }
     *     
     */
    public void setSecurityPropertyInfo(SecurityPropertyType value) {
        this.securityPropertyInfo = value;
    }

    /**
     * Gets the value of the predictionInfo property.
     * 
     * @return
     *     possible object is
     *     {@link PredictionInfoType }
     *     
     */
    public PredictionInfoType getPredictionInfo() {
        return predictionInfo;
    }

    /**
     * Sets the value of the predictionInfo property.
     * 
     * @param value
     *     allowed object is
     *     {@link PredictionInfoType }
     *     
     */
    public void setPredictionInfo(PredictionInfoType value) {
        this.predictionInfo = value;
    }

    /**
     * Gets the value of the extraProperties property.
     * 
     * @return
     *     possible object is
     *     {@link PropertiesType }
     *     
     */
    public PropertiesType getExtraProperties() {
        return extraProperties;
    }

    /**
     * Sets the value of the extraProperties property.
     * 
     * @param value
     *     allowed object is
     *     {@link PropertiesType }
     *     
     */
    public void setExtraProperties(PropertiesType value) {
        this.extraProperties = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setType(String value) {
        this.type = value;
    }

}
