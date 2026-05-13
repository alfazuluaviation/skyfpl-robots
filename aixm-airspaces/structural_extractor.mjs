import fs from 'fs';
import axios from 'axios';
import JSZip from 'jszip';
import { XMLParser } from 'fast-xml-parser';
import { createClient } from '@supabase/supabase-base';
import dotenv from 'dotenv';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function toTacticalCase(str) {
    if (!str) return '';
    return str.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
}

function extractAllNotes(timeSlice) {
    const notes = [];
    if (timeSlice.annotation?.Note?.translatedNote) {
        const tNotes = Array.isArray(timeSlice.annotation.Note.translatedNote) 
            ? timeSlice.annotation.Note.translatedNote 
            : [timeSlice.annotation.Note.translatedNote];
        
        tNotes.forEach(tn => {
            if (tn.LinguisticNote?.note?.['#text']) notes.push(tn.LinguisticNote.note['#text']);
            else if (tn.LinguisticNote?.note) notes.push(tn.LinguisticNote.note);
        });
    }
    return notes;
}

async function runSync() {
    console.log('🚀 [ROBOT-STRUCTURAL] Iniciando Super Varredura de TMA, CTR, FIR, CTA...');
    
    try {
        console.log('🔍 [ROBOT] Consultando catálogo oficial via Edge Function...');
        const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
        const discoveryRes = await axios.post(DISCOVERY_URL, 
            { area: 'pub', type: 'aixm' },
            { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
        );

        if (!discoveryRes.data?.success) throw new Error('Falha ao descobrir links.');

        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(discoveryRes.data.xml);
        const items = jsonObj.aisweb?.pub?.item || [];
        const itemsArray = Array.isArray(items) ? items : [items];
        
        console.log(`🚨 [AUDITORIA] Encontrados ${itemsArray.length} pacotes no catálogo.`);

        const serviceMap = {}; // designator/name -> frequencies[]
        const enrichedData = [];

        // PROCESSAR TODOS OS PACOTES DO CATÁLOGO
        for (let i = 0; i < itemsArray.length; i++) {
            const item = itemsArray[i];
            console.log(`📥 [ROBOT] Processando Pacote [${i}]: ${item.name}...`);
            
            let link = item.link || item.file || '';
            if (typeof link === 'object') link = link['#text'] || '';
            link = link.replace(']]>', '').replace('<![CDATA[', '').split('">')[0].trim();

            const tempPath = `./temp_aixm_${i}.zip`;
            const curlCmd = `curl -L -A "Mozilla/5.0" -o ${tempPath} "${link}"`;
            
            try {
                require('child_process').execSync(curlCmd);
                const zipData = fs.readFileSync(tempPath);
                const zip = await JSZip.loadAsync(zipData);
                fs.unlinkSync(tempPath);

                const xmlFiles = Object.keys(zip.files).filter(f => f.endsWith('.xml'));
                for (const xmlFileName of xmlFiles) {
                    console.log(`   📄 Lendo: ${xmlFileName}`);
                    const xmlText = await zip.files[xmlFileName].async('string');
                    const parser2 = new XMLParser({ 
                        ignoreAttributes: false, 
                        attributeNamePrefix: "@_", 
                        removeNSPrefix: true,
                        alwaysArray: ["hasMember", "timeSlice", "radioCommunicationChannel"]
                    });
                    const jsonObj2 = parser2.parse(xmlText);
                    const members = jsonObj2.AIXMBasicMessage?.hasMember || [];

                    members.forEach(member => {
                        // 1. Busca Literal de Frequências (Scanner de Deep Object)
                        const foundFreqs = [];
                        const deepScan = (obj) => {
                            if (!obj) return;
                            if (typeof obj === 'string' || typeof obj === 'number') {
                                const val = String(obj);
                                if (val.includes('.') || val.includes(',')) {
                                    const num = parseFloat(val.replace(',', '.'));
                                    if (num >= 108.0 && num <= 137.0) foundFreqs.push(`${num.toFixed(3)} MHz`);
                                }
                                return;
                            }
                            if (Array.isArray(obj)) { obj.forEach(deepScan); return; }
                            if (typeof obj === 'object') {
                                for (const k in obj) deepScan(obj[k]);
                            }
                        };
                        deepScan(member);

                        // 2. Indexar Frequências se for Serviço/Unidade
                        const entity = Object.values(member)[0];
                        const tSlices = Array.isArray(entity?.timeSlice) ? entity.timeSlice : (entity?.timeSlice ? [entity.timeSlice] : []);
                        
                        if (foundFreqs.length > 0) {
                            tSlices.forEach(ts => {
                                const data = ts?.ServiceTimeSlice || ts?.UnitTimeSlice || ts?.AirTrafficControlServiceTimeSlice || ts;
                                if (!data) return;
                                [data.designator, data.name].filter(Boolean).forEach(k => {
                                    const nk = k.toString().toUpperCase();
                                    if (!serviceMap[nk]) serviceMap[nk] = [];
                                    serviceMap[nk].push(...foundFreqs);
                                });
                            });
                        }

                        // 3. Coletar Espaços Aéreos
                        const airspace = member.Airspace;
                        if (airspace) {
                            const aSlices = Array.isArray(airspace.timeSlice) ? airspace.timeSlice : (airspace.timeSlice ? [airspace.timeSlice] : []);
                            const ts = aSlices.find(s => ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(s.AirspaceTimeSlice?.interpretation))?.AirspaceTimeSlice;
                            if (ts && ['TMA', 'CTR', 'FIR', 'CTA'].includes(ts.type)) {
                                const ident = String(ts.designator || '');
                                // Evitar duplicatas entre pacotes (manter o mais recente)
                                if (!enrichedData.find(e => e.ident === ident)) {
                                    enrichedData.push({
                                        ident,
                                        nam: ts.name || '',
                                        type: (ts.type === 'CTA' || ts.type === 'FIR') ? 'TMA' : ts.type,
                                        originalType: ts.type,
                                        upperLimit: ts.upperLimit?.val || ts.upperLimit,
                                        uom_upper: ts.upperLimit?.['@_uom'] || 'FL',
                                        lowerLimit: ts.lowerLimit?.val || ts.lowerLimit,
                                        uom_lower: ts.lowerLimit?.['@_uom'] || 'FL',
                                        horario: 'H24',
                                        observacoes: toTacticalCase(extractAllNotes(ts).join(' / ')) || 'SEM OBSERVAÇÕES',
                                        raw: ts
                                    });
                                }
                            }
                        }
                    });
                }
            } catch (err) {
                console.error(`⚠️ Erro ao processar pacote [${i}]: ${err.message}`);
            }
        }

        console.log(`📡 [ROBOT] Varredura finalizada. ${Object.keys(serviceMap).length} órgãos indexados.`);
        console.log(`🌍 [ROBOT] Sincronizando ${enrichedData.length} áreas estruturais...`);

        const normalize = (str) => str?.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/[^A-Z0-9]/g, "") || '';

        for (const area of enrichedData) {
            const normIdent = normalize(area.ident);
            const normName = normalize(area.nam);
            
            // Busca de Frequências (Match por Nome/Designador ou Prefixo)
            let freqs = serviceMap[area.ident?.toUpperCase()] || [];
            if (freqs.length === 0) {
                const matchingKey = Object.keys(serviceMap).find(k => {
                    const nk = normalize(k);
                    if (nk.includes(normIdent) || normIdent.includes(nk)) return true;
                    if (nk.includes(normName) || normName.includes(nk)) return true;
                    if (normName.length >= 5 && nk.startsWith(normName.substring(0, 5))) return true;
                    return false;
                });
                if (matchingKey) freqs = serviceMap[matchingKey];
            }

            const finalFrequencies = [...new Set(freqs)];

            // Fallback Regex nas Notas (Limpeza RTF)
            if (finalFrequencies.length === 0 && area.observacoes) {
                const cleanObs = area.observacoes.replace(/\\['][a-f0-9]{2}/g, '').replace(/\\[a-z0-9]+/g, ' ').replace(/[{}]/g, '');
                const freqRegex = /(\d{3}[.,]\d{2,3})/g;
                let m;
                while ((m = freqRegex.exec(cleanObs)) !== null) {
                    const f = m[1].replace(',', '.');
                    const num = parseFloat(f);
                    if (num >= 108 && num <= 137) finalFrequencies.push(`${num.toFixed(3)} MHz`);
                }
            }

            // Sincronizar Supabase
            const { data: existing } = await supabase.from('airspace_snapshots').select('id, raw_properties').eq('ident', area.ident).eq('is_current', true).maybeSingle();

            const snapshotData = {
                ident: area.ident,
                nam: area.nam,
                type: area.type,
                upperlimit: parseInt(area.upperLimit) || null,
                uplimituni: area.uom_upper,
                lowerlimit: parseInt(area.lowerLimit) || null,
                lowerlimituni: area.uom_lower,
                is_current: true,
                raw_properties: {
                    ...(existing?.raw_properties || {}),
                    aip_data: {
                        horario: area.horario,
                        observacoes: area.observacoes,
                        frequencias: [...new Set(finalFrequencies)],
                        full_aixm_node: area.raw
                    }
                }
            };

            if (existing) {
                await supabase.from('airspace_snapshots').update(snapshotData).eq('id', existing.id);
            } else {
                await supabase.from('airspace_snapshots').insert(snapshotData);
            }
        }

        console.log('✅ [ROBOT] Super Varredura concluída com sucesso!');
    } catch (error) {
        console.error('❌ [ROBOT] Erro fatal:', error.message);
    }
}

runSync();
