import fs from 'fs';
import { XMLParser } from 'fast-xml-parser';

const parser = new XMLParser({ 
    ignoreAttributes: false, 
    attributeNamePrefix: "@_", 
    removeNSPrefix: true,
    alwaysArray: ["hasMember", "timeSlice"]
});

// Procurar o arquivo XML que contém SBWJ
const files = fs.readdirSync('c:/Users/josemir/Desktop/skyfpl-robots_temp/aixm-airspaces').filter(f => f.endsWith('.xml'));

for (const file of files) {
    const xml = fs.readFileSync(`c:/Users/josemir/Desktop/skyfpl-robots_temp/aixm-airspaces/${file}`, 'utf8');
    if (xml.includes('SBWJ')) {
        const jsonObj = parser.parse(xml);
        const members = jsonObj.AIXMBasicMessage?.hasMember || [];
        const sbwj = members.find(m => {
            const ts = m.Airspace?.timeSlice?.[0]?.AirspaceTimeSlice;
            return ts?.designator === 'SBWJ_02' || ts?.designator === 'SBWJ_03';
        });
        if (sbwj) {
            console.log(JSON.stringify(sbwj, null, 2));
            break;
        }
    }
}
