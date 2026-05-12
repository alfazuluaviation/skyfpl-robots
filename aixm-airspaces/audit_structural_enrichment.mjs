import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
import path from 'path';

// Carregar credenciais do Admin Dashboard
dotenv.config({ path: '../skynav-pro-official/admin/.env.local' });

const SUPABASE_URL = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.VITE_SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('❌ Erro: Credenciais do Supabase não encontradas.');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function auditEnrichment() {
    console.log('🛰️ [AUDITORIA] Analisando dados enriquecidos pelo Robô Estrutural...\n');

    const { data: snapshots, error } = await supabase
        .from('airspace_snapshots')
        .select('ident, type, raw_properties, is_current')
        .eq('is_current', true)
        .order('type', { ascending: true })
        .order('ident', { ascending: true });

    if (error) {
        console.error('❌ Erro ao consultar snapshots:', error.message);
        return;
    }

    const stats = {
        TMA: { total: 0, enriched: 0 },
        CTR: { total: 0, enriched: 0 },
        FIR: { total: 0, enriched: 0 },
        CTA: { total: 0, enriched: 0 },
    };

    const enrichedSamples = [];

    snapshots.forEach(s => {
        if (stats[s.type]) {
            stats[s.type].total++;
            const aipData = s.raw_properties?.aip_data;
            if (aipData) {
                stats[s.type].enriched++;
                if (enrichedSamples.length < 15) {
                    enrichedSamples.push({
                        ident: s.ident,
                        type: s.type,
                        horario: aipData.horario,
                        freqs: (aipData.frequencias || []).join(', '),
                        obs: (aipData.observacoes || '').substring(0, 50) + '...'
                    });
                }
            }
        }
    });

    console.log('📊 Resumo de Enriquecimento:');
    console.table(Object.entries(stats).map(([type, data]) => ({
        Tipo: type,
        Total: data.total,
        Enriquecidos: data.enriched,
        'Cobertura (%)': data.total > 0 ? ((data.enriched / data.total) * 100).toFixed(1) + '%' : '0%'
    })));

    console.log('\n🧠 Amostras de Dados Enriquecidos:');
    console.table(enrichedSamples);

    // Investigar áreas ignoradas (exemplo: FIRs)
    const ignoredFirs = ['SBCW', 'SBAO', 'SBAZ', 'SBBS', 'SBRE'];
    console.log('\n🔍 Investigando FIRs ignoradas (SBCW, SBAO, etc.):');
    
    for (const fir of ignoredFirs) {
        const { data: records } = await supabase
            .from('airspace_snapshots')
            .select('ident, type, is_current')
            .eq('ident', fir);
        
        if (!records || records.length === 0) {
            console.log(`❌ FIR ${fir}: Não existe na tabela airspace_snapshots.`);
        } else {
            const current = records.find(r => r.is_current);
            console.log(`⚠️ FIR ${fir}: Encontrada (${records.length} registros). Ativa (is_current)? ${current ? 'SIM' : 'NÃO'}`);
        }
    }
}

auditEnrichment().catch(console.error);
