<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:sev="http://www.eos.ru/2010/sev">

    <xsl:template match="/xdms:communication">
        <sev:Report xmlns:sev="http://www.eos.ru/2010/sev"> 
            <xsl:apply-templates/>
        </sev:Report>
    </xsl:template>

    <xsl:template match="xdms:header">
        <sev:Header MessageType="Failure" ReturnID="$RETURN_UID" ResourceID="0" Version="1.0">
            
            <xsl:attribute name="MessageID">
                <xsl:value-of select="./@xdms:uid" />
            </xsl:attribute>

            <xsl:attribute name="Time">
                <xsl:value-of select="./@xdms:created" />
            </xsl:attribute>            

            <xsl:apply-templates/>
        </sev:Header>
    </xsl:template>

    <xsl:template match="xdms:source">
        <sev:Sender>
            <sev:Contact>
            </sev:Contact>
            <sev:EDMS UID="4313BCFDAD6A422EA375EF34BB248BCD" Version="12.0.0"/>
        </sev:Sender>

        <sev:Recipient>
            <sev:Contact>
            </sev:Contact>            
        </sev:Recipient>

        <sev:ResourceList>
            <sev:Resource UID="0" UniqueName="Report.xml"/>
        </sev:ResourceList>
    </xsl:template>

    <xsl:template match="xdms:notification">
        <sev:Notification>
            <sev:InitialDoc UID="$DOCUMENT_UID">
                <sev:Number>
                    <xsl:value-of select="./xdms:documentRefused/xdms:foundation/xdms:num/xdms:number" />
                </sev:Number>
                <sev:Date>
                    <xsl:value-of select="./xdms:documentRefused/xdms:foundation/xdms:num/xdms:date" />
                </sev:Date>
                <sev:Group>$DOCUMENT_GROUP</sev:Group>
            </sev:InitialDoc>

            <sev:Failure Code="39">
                <xsl:value-of select="./xdms:documentRefused/xdms:reason" />
            </sev:Failure>
        </sev:Notification>
 
    </xsl:template>
</xsl:stylesheet>