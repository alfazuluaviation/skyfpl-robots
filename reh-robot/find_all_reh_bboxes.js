const https = require('https');

const url = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities';

https.get(url, (res) => {
    let data = [];
    res.on('data', chunk => data.push(chunk));
    res.on('end', () => {
        const xml = Buffer.concat(data).toString();
        const layerBlocks = xml.split('<Layer');
        layerBlocks.forEach(block => {
            const nameMatch = block.match(/<Name>([^<]+)<\/Name>/);
            if (!nameMatch) return;
            const name = nameMatch[1];
            
            // Filtro para capturar todas as camadas relevantes
            if (name.includes('REH') || name.startsWith('REH_') || name.includes('CCV_REH') || name === 'REH_BACIA_DE_SANTOS' || name === 'REH_CURITIBA' || name === 'REH_VITORIA') {
                const bboxMatch = block.match(/LatLonBoundingBox[^>]+minx="([^"]+)"[^>]+miny="([^"]+)"[^>]+maxx="([^"]+)"[^>]+maxy="([^"]+)"/);
                if (bboxMatch) {
                    console.log(`"${name}": [${bboxMatch[1]}, ${bboxMatch[2]}, ${bboxMatch[3]}, ${bboxMatch[4]}],`);
                } else {
                    console.log(`"${name}": NO BBOX`);
                }
            }
        });
    });
});
