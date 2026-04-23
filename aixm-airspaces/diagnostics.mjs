import fs from 'fs';
import path from 'path';
import axios from 'axios';
import JSZip from 'jszip';
import { XMLParser } from 'fast-xml-parser';

// Read env from admin
const envPath = path.resolve('../../skynav-pro-official/admin/.env.local');
const envFile = fs.readFileSync(envPath, 'utf-8');
let SUPABASE_URL = '', SUPABASE_KEY = '';
envFile.split('\n').forEach(line => {
    if (line.startsWith('VITE_SUPABASE_URL=')) SUPABASE_URL = line.split('=', 2)[1].trim();
    if (line.startsWith('VITE_SUPABASE_SERVICE_ROLE_KEY=')) SUPABASE_KEY = line.split('=', 2)[1].trim();
});

async function analyze() {
    console.log('🔍 [DIAGNOSTICO] Iniciando fetch...');
    const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
    const discoveryRes = await axios.post(DISCOVERY_URL, 
        { area: 'pub', type: 'aixm' },
        { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
    );

    const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
    const jsonObj = parser.parse(discoveryRes.data.xml);
    const items = jsonObj.aisweb?.pub?.item || [];
    const itemsArray = Array.isArray(items) ? items : [items];

    const selectedItem = itemsArray.find(item => {
        const name = String(item.name || '').toLowerCase();
        return name.includes('completo') || name.includes('snapshot') || name.includes('full');
    }) || itemsArray[0];

    let dynamicLink = selectedItem?.link || selectedItem?.file || '';
    if (typeof dynamicLink === 'object') dynamicLink = dynamicLink['#text'] || '';
    dynamicLink = dynamicLink.replace(']]>', '').replace('<![CDATA[', '').split('">')[0].trim();

    console.log(`📦 Baixando: ${dynamicLink}`);
    const response = await axios.get(dynamicLink, { responseType: 'arraybuffer' });
    
    const zip = await JSZip.loadAsync(response.data);
    const xmlFile = Object.values(zip.files).find(f => f.name.endsWith('.xml'));
    const xmlData = await xmlFile.async('string');

    console.log(`🧠 Analisando SBR602...`);
    const parsed = parser.parse(xmlData);
    const members = parsed?.AIXMBasicMessage?.hasMember || parsed?.message?.hasMember || [];
    
    let found = false;
    for (const member of members) {
        const airspace = member?.Airspace;
        if (airspace) {
            const timeSlices = Array.isArray(airspace.timeSlice) ? airspace.timeSlice : [airspace.timeSlice];
            const ts = timeSlices.find(t => 
                ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(t.AirspaceTimeSlice?.interpretation)
            )?.AirspaceTimeSlice || timeSlices[0]?.AirspaceTimeSlice;
            
            if (ts && ts.designator === 'SBR602') {
                console.log('--- ENCONTRADO SBR602 ---');
                console.log(JSON.stringify(ts, null, 2));
                found = true;
                break;
            }
        }
    }
    if (!found) console.log('SBR602 não encontrado!');
}

analyze().catch(console.error);
