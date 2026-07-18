import { createClient } from '@supabase/supabase-js';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import * as dotenv from 'dotenv';
import { createRequire } from 'module';
import { readFile } from 'fs/promises';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Fix para Node.js 20: WebSocket nativo não suportado pelo @supabase/realtime-js
const require = createRequire(import.meta.url);
const ws = require('ws');

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── Supabase ──────────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL || '',
    process.env.SUPABASE_SERVICE_ROLE_KEY || '',
    { realtime: { transport: ws } }  // Fix Node.js 20
);

// ── Cloudflare R2 (via S3 API) ────────────────────────────────────────────────
const s3 = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT || '',
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || ''
    }
});
const BUCKET_NAME = 'skyfpl-charts';

// ── Constantes ────────────────────────────────────────────────────────────────
const DELAY_MS = 2000;         // 2 segundos entre chamadas ao DECEA (3s bate no limite de 6h do GitHub Actions)
const DAYS_BEFORE_CYCLE = 2;   // Quantos dias antes do novo ciclo o robô deve rodar
const BATCH_SIZE = 50;         // Aeródromos por log de progresso

// ── Modo de Teste ─────────────────────────────────────────────────────────────
const FORCE_RUN = process.env.FORCE_RUN === 'true'; // Ignora a verificação de data AIRAC
const MAX_AERODROMES = parseInt(process.env.MAX_AERODROMES || '0', 10); // 0 = sem limite

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ── INTELIGÊNCIA AIRAC ────────────────────────────────────────────────────────
// Lê o mesmo calendar.json que governa todo o ecossistema SkyFPL
function loadAiracCalendar() {
    const calendarPath = path.join(__dirname, 'calendar.json');
    // Leitura síncrona para simplicidade no início do script
    const raw = require('fs').readFileSync(calendarPath, 'utf-8');
    return JSON.parse(raw);
}

function findNextAiracCycle(calendar) {
    const now = new Date();
    const allCycles = [];

    for (const year of Object.keys(calendar)) {
        for (const [cycle, dateStr] of Object.entries(calendar[year])) {
            // Formato DD/MM/YYYY → Date
            const [d, m, y] = dateStr.split('/');
            const date = new Date(Number(y), Number(m) - 1, Number(d));
            allCycles.push({ cycle, date, dateStr });
        }
    }

    // Ordena do mais próximo ao mais distante
    allCycles.sort((a, b) => a.date - b.date);

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    // Ciclo atual = maior data que já passou (estritamente menor que hoje)
    const current = allCycles.filter(c => c.date < today).at(-1);
    // Próximo ciclo (ou o ciclo que começa hoje)
    const next = allCycles.find(c => c.date >= today);

    return { current, next };
}

function shouldRunToday(nextCycle) {
    if (!nextCycle) return false;
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const cycleDate = new Date(nextCycle.date);
    cycleDate.setHours(0, 0, 0, 0);

    const diffDays = Math.round((cycleDate - today) / (1000 * 60 * 60 * 24));
    console.log(`📅 Próximo ciclo AIRAC: ${nextCycle.cycle} em ${nextCycle.dateStr} (${diffDays} dia(s) restantes)`);

    return diffDays <= DAYS_BEFORE_CYCLE && diffDays >= 0;
}

// ── MOTOR PRINCIPAL ───────────────────────────────────────────────────────────
async function startCrawler() {
    console.log('═══════════════════════════════════════════════════════════');
    console.log('🤖  SkyFPL ROTAER Crawler — Iniciando');
    console.log(`🕐  ${new Date().toISOString()}`);
    console.log('═══════════════════════════════════════════════════════════');

    // 1. Verificar se é hora de rodar (inteligência AIRAC)
    const calendar = loadAiracCalendar();
    const { current, next } = findNextAiracCycle(calendar);

    console.log(`\n🛫 Ciclo Atual : ${current?.cycle || 'N/A'} (${current?.dateStr || 'N/A'})`);

    if (FORCE_RUN) {
        console.log(`\n🧪 MODO DE TESTE ATIVADO (FORCE_RUN=true)`);
        console.log(`   Verificação de data AIRAC ignorada.`);
        if (MAX_AERODROMES > 0) {
            console.log(`   Limite de processamento: ${MAX_AERODROMES} aeródromo(s).`);
        }
    } else if (!shouldRunToday(next)) {
        console.log(`\n✅ Nenhuma ação necessária hoje. O robô voltará a verificar amanhã.`);
        console.log('   (O próximo ciclo ainda está longe. Encerrando com custo zero.)');
        process.exit(0);
    } else {
        console.log(`\n🚨 JANELA DE ATUALIZAÇÃO DETECTADA!`);
        console.log(`   Ciclo ${next.cycle} entra em vigor em ${next.dateStr}.`);
        console.log(`   Iniciando raspagem completa do ROTAER...\n`);
    }

    // 2. Buscar lista de todos os aeródromos e helipontos do Cloudflare R2
    console.log('📡 Buscando lista da malha aérea (latest_navdata.json) no Cloudflare R2...');
    let targets = [];
    try {
        const response = await fetch('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json');
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const navdata = await response.json();
        
        targets = (navdata.data || [])
            .filter(a => (a.type === 'airport' || a.type === 'heliport') && a.icao)
            .map(a => ({
                icao: a.icao,
                ciad: a.props?.ciad || '',
                name: a.name || a.props?.nome || '',
                type: a.type
            }))
            .sort((a, b) => a.icao.localeCompare(b.icao));
            
        console.log(`✅ ${targets.length} aeródromos válidos filtrados.`);
    } catch (error) {
        console.error(`❌ Erro ao baixar latest_navdata.json: ${error.message}`);
        process.exit(1);
    }

    // Aplica limite de teste se MAX_AERODROMES estiver definido
    if (MAX_AERODROMES > 0 && targets.length > MAX_AERODROMES) {
        console.log(`🧪 Modo teste: limitando de ${targets.length} para ${MAX_AERODROMES} aeródromo(s)`);
        targets = targets.slice(0, MAX_AERODROMES);
    }

    console.log(`✈️  Total de alvos a processar: ${targets.length}`);
    console.log(`⏱️  Tempo estimado: ~${Math.round((targets.length * DELAY_MS) / 1000)} segundos\n`);

    // 3. Loop de raspagem
    const results = {};
    let successCount = 0;
    let failCount = 0;
    const startTime = Date.now();

    for (const [index, aero] of targets.entries()) {
        const icao = aero.icao;
        const progress = `[${index + 1}/${targets.length}]`;

        // Log de progresso a cada BATCH_SIZE aeródromos
        if (index % BATCH_SIZE === 0 && index > 0) {
            const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
            const rate = (successCount / (index + 1) * 100).toFixed(0);
            console.log(`\n📊 Progresso: ${progress} | ✅ ${successCount} | ❌ ${failCount} | ⏱️ ${elapsed} min | Taxa: ${rate}%\n`);
        }

        process.stdout.write(`  ${progress} ${icao.padEnd(6)} → `);

        try {
            const { data, error: fnError } = await supabase.functions.invoke('fetch-rotaer', {
                method: 'POST',
                body: { icaoCode: icao }
            });

            if (fnError) throw new Error(fnError.message || JSON.stringify(fnError));
            if (!data?.success || !data?.data) throw new Error('Resposta sem dados válidos');

            results[icao] = {
                ...data.data,
                _airac_cycle: next.cycle,
                _crawled_at: new Date().toISOString()
            };
            successCount++;
            process.stdout.write(`✅ OK\n`);

        } catch (e) {
            process.stdout.write(`❌ FALHA (${e.message.substring(0, 60)})\n`);
            failCount++;
        }

        // Sleep entre requisições (proteção de rate limit do DECEA)
        if (index < targets.length - 1) {
            await sleep(DELAY_MS);
        }
    }

    // 4. Relatório final de raspagem
    const totalTime = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('📊 RELATÓRIO FINAL DA RASPAGEM');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`   ✅ Sucessos  : ${successCount}`);
    console.log(`   ❌ Falhas    : ${failCount}`);
    console.log(`   ⏱️  Tempo total: ${totalTime} minutos`);
    console.log(`   📦 Ciclo     : ${next.cycle} (efetivo em ${next.dateStr})`);

    if (successCount === 0) {
        console.error('\n❌ Nenhum dado foi coletado. Abortando upload para evitar sobrescrever dados bons.');
        process.exit(1);
    }

    // 5. Montar e publicar o Snapshot Offline no Cloudflare R2
    console.log('\n☁️  Publicando snapshot no Cloudflare R2...');
    const snapshot = JSON.stringify({
        _meta: {
            generated_at: new Date().toISOString(),
            airac_cycle: next.cycle,
            airac_effective_date: next.dateStr,
            total_success: successCount,
            total_fail: failCount,
            version: '1.0.0'
        },
        data: results
    });

    const R2_KEY = `rotaer/rotaer_${next.cycle}_snapshot.json`;
    const R2_KEY_LATEST = 'rotaer/rotaer_snapshot_latest.json';

    try {
        // Upload da versão AIRAC específica (ex: rotaer_2607_snapshot.json)
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: R2_KEY,
            Body: snapshot,
            ContentType: 'application/json',
            CacheControl: 'public, max-age=2419200' // 28 dias (1 ciclo AIRAC)
        }));
        console.log(`   ✅ Publicado: ${R2_KEY}`);

        // Upload da versão LATEST (sempre aponta para o ciclo mais recente)
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: R2_KEY_LATEST,
            Body: snapshot,
            ContentType: 'application/json',
            CacheControl: 'public, max-age=86400' // 1 dia (pode ser atualizado)
        }));
        console.log(`   ✅ Publicado: ${R2_KEY_LATEST}`);

    } catch (e) {
        console.error(`   ❌ Erro no upload para R2: ${e.message}`);
        process.exit(1);
    }

    console.log('\n🏁 Crawler finalizado com sucesso!');
    console.log(`   URL disponível para o App Nativo:`);
    console.log(`   https://cartas.skyfpl.com/${R2_KEY_LATEST}`);
    console.log('═══════════════════════════════════════════════════════════\n');
}

startCrawler().catch(err => {
    console.error('\n💥 CRASH FATAL:', err);
    process.exit(1);
});
