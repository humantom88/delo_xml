<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:sev="http://www.eos.ru/2010/sev">

    <xsl:template match="/xdms:communication">
        <sev:Report xmlns:sev="http://www.eos.ru/2010/sev"> 
            <xsl:apply-templates/>
        </sev:Report>
    </xsl:template>

    <xsl:template match="xdms:header">
        <sev:Header MessageType="Report" Version="1.0" ReturnID="$RETURN_UID" ResourceID="0">
            
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
                    <xsl:value-of select="./xdms:documentAccepted/xdms:foundation/xdms:num/xdms:number" />
                </sev:Number>
                <sev:Date>
                    <xsl:value-of select="./xdms:documentAccepted/xdms:foundation/xdms:num/xdms:date" />
                </sev:Date>
                <sev:Group>$DOCUMENT_GROUP</sev:Group>
            </sev:InitialDoc>

            <sev:Registration SystemDate="$DATETIME">
                <xsl:attribute name="DocumentID">
                    <xsl:value-of select="./@xdms:id" />
                </xsl:attribute>
            </sev:Registration>
        </sev:Notification>
 
        <sev:DocumentList>
            <sev:Document MainDocument="false" Type="Incoming" UID="$DOCUMENT_UID">

                <xsl:attribute name="DocumentID">
                    <xsl:value-of select="./@xdms:id" />
                </xsl:attribute>

                <sev:RegistrationInfo>
                    <sev:Number>
                        <xsl:value-of select="./xdms:documentAccepted/xdms:num/xdms:number"/>
                    </sev:Number>
                    <sev:Date>
                        <xsl:value-of select="./xdms:documentAccepted/xdms:num/xdms:date"/>
                    </sev:Date>
                </sev:RegistrationInfo>
            </sev:Document>
        </sev:DocumentList>
    </xsl:template>
</xsl:stylesheet>