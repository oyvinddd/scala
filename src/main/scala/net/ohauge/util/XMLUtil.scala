package net.ohauge.util

import scala.util.Try

object XMLUtil extends App {

    val fusjonXML = <fusjon
    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='http://kunnxml.brreg.no/fr/schema/FUSJ-2.xsd' register='FR' kunngjoringstype='FUSJ' kreditorvarsel='J' rettelse='N' overskrift='Kreditorvarsel'>
        <header>Fusjon</header>
        <body>
            <overtaker>
                <tekstLinje linjenr='1'>Overtakende foretak</tekstLinje>
                <selskap rekkefolge='1'>
                    <navn ledetekst='Foretaksnavn'>GP EIENDOM FAGERHOLT AS</navn>
                    <organisasjonsnummer ledetekst='Organisasjonsnummer'>891617372</organisasjonsnummer>
                    <forretningsadresse ledetekst='Forretningsadresse'>
                        <adresse>Vige havnevei 56</adresse>
                        <poststed>4633 KRISTIANSAND S</poststed>
                    </forretningsadresse>
                    <kommune ledetekst='Kommune'>1001 KRISTIANSAND</kommune>
                    <organisasjonsform ledetekst='Organisasjonsform'>Aksjeselskap</organisasjonsform>
                </selskap>
            </overtaker>
            <beslutning>
                <tekstLinje linjenr='1'>Foretaket har besluttet å innfusjonere:</tekstLinje>
            </beslutning>
            <overdrager>
                <tekstLinje linjenr='1'>Overdragende foretak</tekstLinje>
                <selskap rekkefolge='1'>
                    <navn ledetekst='Foretaksnavn'>GP EIENDOM FAGERHOLT II AS</navn>
                    <organisasjonsnummer ledetekst='Organisasjonsnummer'>993189774</organisasjonsnummer>
                    <forretningsadresse ledetekst='Forretningsadresse'>
                        <adresse>Vige havnevei 56</adresse>
                        <poststed>4633 KRISTIANSAND S</poststed>
                    </forretningsadresse>
                    <kommune ledetekst='Kommune'>1001 KRISTIANSAND</kommune>
                    <organisasjonsform ledetekst='Organisasjonsform'>Aksjeselskap</organisasjonsform>
                </selskap>
            </overdrager>
            <kreditorfrist>
                <tekstLinje linjenr='1'>Eventuelle krav/innsigelser må meldes foretaket innen seks uker regnet fra 19.09.2017.</tekstLinje>
            </kreditorfrist>
        </body>
        <footer>Foretaksregisteret 19.09.2017</footer>
    </fusjon>

    val fusjonXMLStr = "            <overtaker>\n                <tekstLinje linjenr='1'>Overtakende foretak</tekstLinje>\n                <selskap rekkefolge='1'>\n                    <navn ledetekst='Foretaksnavn'>GP EIENDOM FAGERHOLT AS</navn>\n                    <organisasjonsnummer ledetekst='Organisasjonsnummer'>891617372</organisasjonsnummer>\n                    <forretningsadresse ledetekst='Forretningsadresse'>\n                        <adresse>Vige havnevei 56</adresse>\n                        <poststed>4633 KRISTIANSAND S</poststed>\n                    </forretningsadresse>\n                    <kommune ledetekst='Kommune'>1001 KRISTIANSAND</kommune>\n                    <organisasjonsform ledetekst='Organisasjonsform'>Aksjeselskap</organisasjonsform>\n                </selskap>\n            </overtaker>"

    def extractValueFromXML(xmlString: String, subElement: String, needle: String): String = {
        val xml = Try(scala.xml.XML.loadString(xmlString)).getOrElse(null)
        if(xml == null) return null
        val matchedElements = xml \\ subElement \\ needle
        matchedElements.headOption match {
            case Some(value) => value.text
            case _ => null
        }
    }
}
