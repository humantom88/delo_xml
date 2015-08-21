<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS">
    <xsl:template match="/xdms:communication">
        <sev:DocInfo xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
            <xsl:apply-templates/>
        </sev:DocInfo>
    </xsl:template>

    <xsl:template match="xdms:header" xmlns:sev="http://www.eos.ru/2010/sev">
        <sev:Header Version="1.0" MessageType="MainDoc">
            <xsl:attribute name="MessageID" >
                <xsl:value-of select="./@xdms:uid"/>
            </xsl:attribute>

            <xsl:attribute name="Time">
                <xsl:value-of select="./@xdms:created"/>
            </xsl:attribute>

            <xsl:attribute name="ReturnID">
                <xsl:value-of select="xdms:source/@xdms:uid"/>
            </xsl:attribute>

            <xsl:attribute name="ResourceID">
                <xsl:value-of select="/xdms:communication/xdms:document/@xdms:id"/>
            </xsl:attribute>

            <sev:Sender>
                <sev:Contact>
                    <sev:Organization>
                        <sev:ShortName>
                            <xsl:value-of select="/xdms:communication/xdms:header/xdms:source/xdms:organization"/>
                        </sev:ShortName>
                    </sev:Organization>
                    <sev:OfficialPerson>
                        <sev:FIO>
                            <xsl:value-of select="/xdms:communication/xdms:document/xdms:signatories/xdms:signatory/xdms:person"/>
                        </sev:FIO>
                        <sev:Post>
                            <xsl:value-of select="/xdms:communication/xdms:document/xdms:signatories/xdms:signatory/xdms:organization"/>
                        </sev:Post>
                    </sev:OfficialPerson>
                    <sev:Address>
                        <sev:Settlement>
                            <xsl:value-of select="/xdms:communication/xdms:document/xdms:signatories/xdms:signatory/xdms:region"/>
                        </sev:Settlement>
                    </sev:Address>
                </sev:Contact>
                <sev:EDMS UID="4313BCFDAD6A422EA375EF34BB248BCD" Version="12.0.0"/>
            </sev:Sender>
            <sev:Recipient>
                <sev:Contact>
                    <sev:Organization>
                        <sev:ShortName>
                            <xsl:value-of select="/xdms:communication/xdms:document/xdms:addresees/xdms:addressee/xdms:organization"/>
                        </sev:ShortName>
                    </sev:Organization>
                </sev:Contact>
            </sev:Recipient>
            <sev:ResourceList>
                <sev:Resource UID="0" UniqueName="DocInfo.xml"/>
                <xsl:for-each select = "/xdms:communication/xdms:files/xdms:file">
                    <sev:Resource >
                        <xsl:variable name="UID" select="./@xdms:localId"/>
                        <xsl:variable name="UniqueName" select="./@xdms:localName"/>
        
                        <xsl:attribute name="UID">
                            <xsl:value-of select="$UID + 1" />
                        </xsl:attribute>

                        <xsl:attribute name="UniqueName">
                            <xsl:value-of select="$UniqueName" />
                        </xsl:attribute>

                    </sev:Resource>
                </xsl:for-each>
            </sev:ResourceList>
        </sev:Header>
    </xsl:template>

    <xsl:template match="xdms:document" xmlns:sev="http://www.eos.ru/2010/sev">
    </xsl:template>

    <xsl:template match="xdms:files" xmlns:sev="http://www.eos.ru/2010/sev">
        <sev:DocumentList>
            <sev:Document Type="Incoming" MainDocument='true'>
                <xsl:attribute name="DocumentID">
                    <xsl:value-of select="/xdms:file/@xdms:localId"/>
                </xsl:attribute>
                <sev:RegistrationInfo>
                    <sev:Number>
                        <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:number"/>
                    </sev:Number>
                    <sev:Date>
                        <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:date"/>
                    </sev:Date>
                </sev:RegistrationInfo>
                
                    <xsl:for-each select = "/xdms:communication/xdms:files/xdms:file">
                        <sev:File UID="831ddf8cb62f459dbb78bc02cff0f55f">
                            <xsl:variable name="ResourceID" select="./@xdms:localId"/>

                            <xsl:attribute name="ResourceID">
                                <xsl:value-of select="$ResourceID + 1" />
                            </xsl:attribute>

                            <sev:Description>
                                <xsl:value-of select="./@xdms:localName" />
                            </sev:Description>
                        </sev:File>
                    </xsl:for-each>
        
<!--                 <sev:Description>
                    <xsl:value-of select="/xdms:communication/xdms:document/xdms:annotation"/>
                </sev:Description> -->
            </sev:Document>
        </sev:DocumentList>
    </xsl:template> 
</xsl:stylesheet>