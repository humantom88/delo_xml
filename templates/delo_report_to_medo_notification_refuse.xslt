<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS">
  <xsl:template match="/sev:Report">
    <xdms:communication xdms:version="2.2" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <xdms:header xdms:type="Уведомление" xdms:uid="$MESSAGE_UID">
            <xdms:source xdms:uid="$AGV_GUID">
              <xdms:organization>
                  <xsl:value-of select="/sev:Report/sev:Header/sev:Sender/sev:Contact/sev:Organization/sev:ShortName" />
              </xdms:organization> 
            </xdms:source>
        </xdms:header>
      <xdms:notification xdms:uid="$DOCUMENT_UID" xdms:type="Отказано в регистрации">
         <xdms:documentRefused>
            <xdms:time>
              <xsl:value-of select="/sev:Report/sev:Header/@Time"/>
            </xdms:time>
            <xdms:foundation>
              <xdms:organization>
                    <xsl:value-of select="/sev:Report/sev:Header/sev:Recipient/sev:Contact/sev:Organization/sev:ShortName" />
                </xdms:organization>
              <xdms:num>
                  <xdms:number>
                      <xsl:value-of select="/sev:Report/sev:Notification/sev:InitialDoc/sev:Number"/>
                  </xdms:number>
                  <xdms:date>
                      <xsl:value-of select="/sev:Report/sev:Notification/sev:InitialDoc/sev:Date"/>
                  </xdms:date>
              </xdms:num>
            </xdms:foundation>
            <xdms:correspondent>
                <xdms:region>
                  <xsl:value-of select="/sev:Report/sev:Header/sev:Sender/sev:Contact/sev:Address/sev:Region" />
                </xdms:region>
                <xdms:organization>
                  <xsl:value-of select="/sev:Report/sev:Header/sev:Sender/sev:Contact/sev:Organization/sev:ShortName" />
                </xdms:organization>  
            </xdms:correspondent>
            <xdms:reason>
              <xsl:value-of select="'Не подлежит регистрации'"/>
            </xdms:reason>
        </xdms:documentRefused>
      </xdms:notification> 
    </xdms:communication>
  </xsl:template>
</xsl:stylesheet>