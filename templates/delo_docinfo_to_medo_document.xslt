<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS">

  <xsl:template match="/sev:DocInfo">
    <xdms:communication xdms:version="2.2" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> 
      <xsl:apply-templates/>
    </xdms:communication>
  </xsl:template>

  <xsl:template match="sev:Header" xmlns:xdms="http://www.infpres.com/IEDMS">
    <xdms:header xdms:created="$ISO_DATETIME" xdms:type="Документ" xdms:uid="$TRANSPORT_GUID">

      <xdms:source>
        <xsl:attribute name="xdms:uid">
          <xsl:value-of select="sev:Sender/sev:Contact/sev:Organization/@UID" />
        </xsl:attribute>
        <xdms:organization> <xsl:value-of select="sev:Sender/sev:Contact/sev:Organization/sev:ShortName" /> </xdms:organization>
      </xdms:source>
    </xdms:header>
  </xsl:template>

  <xsl:template match="sev:DocumentList" xmlns:xdms="http://www.infpres.com/IEDMS">
      <xsl:apply-templates/>
  </xsl:template>
    
  <xsl:template match="sev:Document" xmlns:xdms="http://www.infpres.com/IEDMS">
    <xdms:document xdms:uid="$MESSAGE_GUID">

      <xsl:attribute name="xdms:id">
        <xsl:value-of select="./@DocumentID" />
      </xsl:attribute>

      <xdms:num>
        <xdms:number>
          <xsl:value-of select="sev:RegistrationInfo/sev:Number" />
        </xdms:number>
        <xdms:date>
          <xsl:value-of select="sev:RegistrationInfo/sev:Date" />
        </xdms:date>
      </xdms:num>

      <xdms:signatories>
        <xsl:for-each select="sev:Author">
          <xdms:signatory>
  <!--           <xdms:region>Тюменская область</xdms:region> -->
            
            <xsl:if test="sev:Contact/sev:Organization">
              <xdms:organization> 
                <xsl:value-of select="sev:Contact/sev:Organization/sev:ShortName" />
              </xdms:organization>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson">
              <xdms:person> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:FIO" />
              </xdms:person>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:Department">
              <xdms:department> 
                <xsl:value-of select="sev:Contact/sev:Department/sev:Name" />
              </xdms:department>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson">
              <xdms:post> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:Post" />
              </xdms:post>
            </xsl:if>

            <xdms:signed>
                <xsl:value-of select="/sev:DocInfo/sev:DocumentList/sev:Document/sev:Author/sev:RegistrationInfo/sev:Date" />
            </xdms:signed>

          </xdms:signatory>
        </xsl:for-each>
      </xdms:signatories>

      <xdms:addressees>
        <xsl:for-each select="sev:Addressee">
          <xdms:addressee>
              <xsl:if test="sev:Contact/sev:Organization">
              <xdms:organization> 
                <xsl:value-of select="sev:Contact/sev:Organization/sev:ShortName" />
              </xdms:organization>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson">
              <xdms:person> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:FIO" />
              </xdms:person>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:Department">
              <xdms:department> 
                <xsl:value-of select="sev:Contact/sev:Department/sev:Name" />
              </xdms:department>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson/sev:Post">
              <xdms:post> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:Post" />
              </xdms:post>
            </xsl:if>
          </xdms:addressee>
        </xsl:for-each>
      </xdms:addressees>

      <xdms:pages>
        <xsl:value-of select="sev:Consists" />
      </xdms:pages>
        <xdms:annotation>
        <xsl:value-of select="sev:Annotation" />
      </xdms:annotation>


      <xdms:correspondents>
        <xsl:for-each select = "sev:Author">
          <xdms:correspondent>
              <xsl:if test="sev:Contact/sev:Organization">
              <xdms:organization> 
                <xsl:value-of select="sev:Contact/sev:Organization/sev:ShortName" />
              </xdms:organization>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson">
              <xdms:person> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:FIO" />
              </xdms:person>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:Department">
              <xdms:department> 
                <xsl:value-of select="sev:Contact/sev:Department/sev:Name" />
              </xdms:department>
            </xsl:if>

            <xsl:if test="sev:Contact/sev:OfficialPerson">
              <xdms:post> 
                <xsl:value-of select="sev:Contact/sev:OfficialPerson/sev:Post" />
              </xdms:post>
            </xsl:if>

            <xdms:num>
              <xdms:number>
                <xsl:value-of select="../sev:RegistrationInfo/sev:Number" />
              </xdms:number>
              <xdms:date>
                <xsl:value-of select="../sev:RegistrationInfo/sev:Date" />
              </xdms:date>
            </xdms:num>

          </xdms:correspondent>
        </xsl:for-each>
      </xdms:correspondents>
    </xdms:document>
  
    <xdms:files>
      <xsl:for-each select="sev:File">

        <xsl:variable name="localId" select="./@ResourceID"/>
        <xsl:variable name="locname" select="sev:Description"/>

        <xdms:file>
          <xsl:attribute name="xdms:localName">
            <xsl:value-of select="$locname" />
          </xsl:attribute>

          <xsl:attribute name="xdms:localId">
            <xsl:value-of select="$localId - 1" />
          </xsl:attribute>
          
          <xsl:variable name="extens" select="sev:Extension"/>

          <xsl:attribute name="xdms:type">
            <xsl:value-of select="substring-after($extens,'.')" />
          </xsl:attribute>

          <xsl:if test="$localId = 1">
            <xdms:group>Текст документа</xdms:group>
          </xsl:if>            

          <xsl:if test="$localId &gt; 1">
            <xdms:group>Текст приложения</xdms:group>
          </xsl:if>            
          
          <xdms:description>
            <xsl:value-of select="sev:Description" />
          </xdms:description>
        </xdms:file>

      </xsl:for-each>
    </xdms:files>

  </xsl:template>


  <xsl:template match="sev:Subscriptions" xmlns:xdms="http://www.infpres.com/IEDMS">
  </xsl:template>


</xsl:stylesheet>