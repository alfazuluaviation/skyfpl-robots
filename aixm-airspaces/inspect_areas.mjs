import fs from 'fs';
import { XMLParser } from 'fast-xml-parser';

async function inspect() {
    const xmlData = fs.readFileSync('BL_.xml', 'utf-8');
    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: "@_"
    });
    
    console.log('🔍 Analisando XML...');
    const jsonObj = parser.parse(xmlData);
    const members = jsonObj['AIXMBasicMessage']?.hasMember || [];

    const targets = ['SBR614', 'SBR605'];
    
    for (const member of members) {
        const airspace = member.Airspace;
        if (!airspace) continue;

        const timeSlice = airspace.timeSlice?.AirspaceTimeSlice;
        if (!timeSlice) continue;

        const designator = timeSlice.designator?.['#text'] || timeSlice.designator;
        
        if (targets.includes(designator)) {
            console.log(`\n================= AREA: ${designator} =================`);
            console.log('STRUCTURE:', JSON.stringify(timeSlice.geometryComponent?.AirspaceGeometryComponent?.theAirspaceVolume?.AirspaceVolume, null, 2));
            
            const remarks = timeSlice.annotation?.Note;
            console.log('REMARKS:', JSON.stringify(remarks, null, 2));
        }
    }
}

inspect();
