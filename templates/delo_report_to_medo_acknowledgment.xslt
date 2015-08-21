<xsl:stylesheet version="1.0" encoding="utf-8" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:sev="http://www.eos.ru/2010/sev" xmlns:xdms="http://www.infpres.com/IEDMS">
  <xsl:template match="/sev:Report">
    <xdms:communication xdms:version="2.2" xmlns:xdms="http://www.infpres.com/IEDMS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:sev="http://www.eos.ru/2010/sev">
      <xdms:header xdms:type="Квитанция" xdms:uid="$MESSAGE_UID">
        <xdms:source xdms:uid="$AGV_GUID">
          <xdms:organization></xdms:organization>
        </xdms:source>
      </xdms:header>
      <xdms:acknowledgment xdms:uid="$DOCUMENT_UID">
        <xdms:time>
          <xsl:value-of select="/sev:Report/sev:Header/@Time"/>
        </xdms:time>
        <xdms:accepted>true</xdms:accepted>
      </xdms:acknowledgment>
    </xdms:communication>
  </xsl:template>
</xsl:stylesheet>