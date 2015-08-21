<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:sev="http://www.eos.ru/2010/sev">

    <xsl:template match="/xdms:communication">
        <sev:Report xmlns="http://www.eos.ru/2010/sev" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> 
            <xsl:apply-templates/>
        </sev:Report>
    </xsl:template>

    <xsl:template match="xdms:header">
        <sev:Header MessageType="Reception" Version="1.0" ReturnID="$RETURN_UID" ResourceID="0" Time="$DATETIME">
            
            <xsl:attribute name="MessageID">
                <xsl:value-of select="./@xdms:uid" />
            </xsl:attribute>

            <xsl:apply-templates/>
        </sev:Header>
    </xsl:template>

    <xsl:template match="xdms:source">
        <sev:Sender>
            <sev:Contact>
            </sev:Contact>
            <sev:EDMS UID="BCF1A4A8CCC0A243AA56C3F62978E5AB" Version="13.1.0">Дело</sev:EDMS>
        </sev:Sender>

        <sev:Recipient>
            <sev:Contact>
            </sev:Contact>            
        </sev:Recipient>

        <sev:ResourceList>
            <sev:Resource UID="0" UniqueName="Report.xml"/>
        </sev:ResourceList>
    </xsl:template>

    <xsl:template match="xdms:acknowledgment">
        <sev:Notification>
            <sev:InitialDoc UID="$DOCUMENT_UID">
                <sev:Number>$REG_NUMBER</sev:Number>
                <sev:Date>$REG_DATE</sev:Date>
                <sev:Group>$DOCUMENT_GROUP</sev:Group>
            </sev:InitialDoc>
            <sev:Reception>$DATETIME</sev:Reception>
        </sev:Notification>
        <sev:Expansion />
    </xsl:template>

</xsl:stylesheet>