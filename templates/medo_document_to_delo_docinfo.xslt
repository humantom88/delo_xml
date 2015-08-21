<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS">
    <xsl:template match="/xdms:communication">
        <sev:DocInfo xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
            <xsl:apply-templates/>
        </sev:DocInfo>
    </xsl:template>

    <xsl:template match="xdms:header" xmlns:sev="http://www.eos.ru/2010/sev">
        <sev:Header Version="1.0" MessageType="MainDoc" MessageID="$TRANSPORT_UID" ReturnID="$RETURN_UID" ResourceID="0" Time="$TIME">

            <sev:Sender>
                <sev:Contact>
                    <sev:Organization>
                        <xsl:attribute name="UID">
                            <xsl:value-of select="./xdms:source/@xdms:uid" />
                        </xsl:attribute>
                        <sev:ShortName>
                            <xsl:value-of select="/xdms:communication/xdms:header/xdms:source/xdms:organization"/>
                        </sev:ShortName>
                    </sev:Organization>
                </sev:Contact>
                <sev:EDMS UID="4313BCFDAD6A422EA375EF34BB248BCD" Version="12.0.0"/>
            </sev:Sender>
            <sev:Recipient>
                <xsl:for-each select="/xdms:communication/xdms:document/xdms:addressees/xdms:addressee">
                    <sev:Contact>
                        <sev:Organization UID="$ORGANIZATION_UID">
                            <sev:ShortName>
                                $ORGANIZATION_DELONAME"
                            </sev:ShortName>
                        </sev:Organization>
                    </sev:Contact>
                </xsl:for-each>
            </sev:Recipient>
            <sev:ResourceList>
                <sev:Resource UID="0" UniqueName="DocInfo.xml"/>
                <xsl:for-each select = "/xdms:communication/xdms:files/xdms:file">
                    <sev:Resource >
                        <xsl:variable name="UID" select="./@xdms:localId"/>
                        <xsl:variable name="UniqueName" select="./@xdms:localName"/>
                        <xsl:variable name="FileType">
                            <xsl:value-of select="./@xdms:type" />
                        </xsl:variable>

                        <xsl:attribute name="UID">
                            <xsl:value-of select="$UID + 1" />
                        </xsl:attribute>

                        <xsl:attribute name="UniqueName">
                            <xsl:value-of select="concat($UniqueName, '.', $FileType)" />
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
            <sev:Document Type="Created" MainDocument='true' UID="$DOCUMENT_UID">
                <xsl:attribute name="DocumentID">
                    <xsl:value-of select="./xdms:file/@xdms:localId"/>
                </xsl:attribute>
                <sev:RegistrationInfo>
                    <sev:Number>
                        <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:number"/>
                    </sev:Number>
                    <sev:Date>
                        <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:date"/>
                    </sev:Date>
                </sev:RegistrationInfo>
                <sev:Group>$ORGANIZATION_DOCGROUP</sev:Group>
                <sev:Access>общий</sev:Access>
                <sev:Consists>
                    <xsl:value-of select="/xdms:communication/xdms:document/xdms:pages" />
                </sev:Consists>
                <sev:Annotation>
                    <xsl:value-of select="/xdms:communication/xdms:document/xdms:annotation" />
                </sev:Annotation>

                <xsl:for-each select = "/xdms:communication/xdms:files/xdms:file">
                    <sev:File UID="$FILE_UID" Size="0">
                        <xsl:variable name="ResourceID" select="./@xdms:localId"/>

                        <xsl:attribute name="ResourceID">
                            <xsl:value-of select="$ResourceID + 1" />
                        </xsl:attribute>

                        <xsl:variable name="extens">
                            <xsl:value-of select="./@xdms:type" />
                        </xsl:variable>

                        <xsl:variable name="lname">
                            <xsl:value-of select="./@xdms:localName" />
                        </xsl:variable>

                        <sev:Description>
                            <xsl:value-of select="concat($lname,'.', $extens)" />
                        </sev:Description>

                        <sev:Extension>
                            <xsl:value-of select="./@xdms:type" />
                        </sev:Extension>
                    </sev:File>
                </xsl:for-each>

                <xsl:for-each select = "/xdms:communication/xdms:document/xdms:signatories/xdms:signatory">
                    <sev:Author>
                        <sev:Contact>
                            <sev:Organization UID="$ORGANIZATION_UID">
                                <sev:ShortName>
                                    <xsl:value-of select="./xdms:organization" />
                                </sev:ShortName>
                            </sev:Organization>
                            <sev:Department>
                                <sev:Name><xsl:value-of select="./xdms:department" /></sev:Name>
                            </sev:Department>
                            <sev:OfficialPerson>
                                <sev:FIO><xsl:value-of select="./xdms:person" /></sev:FIO>
                                <sev:Post></sev:Post>
                            </sev:OfficialPerson>
                            <sev:Address>
                                <sev:ZipCode></sev:ZipCode>
                                <sev:Region>
                                    <xsl:value-of select="./xdms:region" />
                                </sev:Region>
                                <sev:Settlement></sev:Settlement>
                                <sev:Text></sev:Text>
                            </sev:Address>
                        </sev:Contact>

                        <sev:RegistrationInfo>
                            <sev:Number>
                                <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:number"/>
                            </sev:Number>
                            <sev:Date>
                                <xsl:value-of select="/xdms:communication/xdms:document/xdms:num/xdms:date"/>
                            </sev:Date>
                        </sev:RegistrationInfo>
                    </sev:Author>
                </xsl:for-each>
            </sev:Document>
        </sev:DocumentList>
        <sev:Subscriptions StopDayCount="1">
            <sev:Reception Include="true"/>
            <sev:Registration Include="true"/>
            <sev:Forwarding Include="false"/>
            <sev:Consideration Include="false"/>
            <sev:Report Include="false"/>
            <sev:Redirection Include="true"/>
            <sev:Answer Include="true"/>
            <sev:VisaDirection Include="true"/>
            <sev:SignDirection Include="true"/>
            <sev:VisaInformation Include="true"/>
            <sev:SignInformation Include="true"/>
        </sev:Subscriptions>
    </xsl:template> 
</xsl:stylesheet>
