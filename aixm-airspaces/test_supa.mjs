import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
dotenv.config({ path: '../skynav-pro-official/admin/.env.local' });

const supabaseUrl = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const supabaseKey = process.env.VITE_SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing Supabase credentials');
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function test() {
    console.log('Buscando SBR605...');
    const { data, error } = await supabase
        .from('eac_snapshots')
        .select('ident, raw_properties')
        .eq('ident', 'SBR605')
        .eq('is_current', true)
        .single();
        
    if (error) {
        console.error('Erro:', error);
    } else {
        console.log('Encontrado:', data.ident);
        if (data.raw_properties) {
            console.log('Tem raw_properties:', Object.keys(data.raw_properties));
            console.log('Tem full_aixm_node?', !!data.raw_properties.full_aixm_node);
        } else {
            console.log('Não tem raw_properties.');
        }
    }
}

test();
