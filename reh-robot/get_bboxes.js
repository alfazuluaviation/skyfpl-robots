// Find the correct bboxes for all CCV_REH layers via GetFeatureInfo / GetCapabilities BoundingBox
const https = require('https');

const url = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities';

https.get(url, (res) => {
    let data = [];
    res.on('data', chunk => data.push(chunk));
    res.on('end', () => {
        const xml = Buffer.concat(data).toString();
        // Parse layer blocks to extract LatLonBoundingBox for each CCV_REH layer
        const layerBlocks = xml.split('<Layer');
        layerBlocks.forEach(block => {
            if (!block.includes('CCV_REH')) return;
            const nameMatch = block.match(/<Name>(CCV_REH[^<]+)<\/Name>/);
            if (!nameMatch) return;
            const name = nameMatch[1];
            const bboxMatch = block.match(/LatLonBoundingBox[^>]+minx="([^"]+)"[^>]+miny="([^"]+)"[^>]+maxx="([^"]+)"[^>]+maxy="([^"]+)"/);
            if (bboxMatch) {
                console.log(`${name}: [${bboxMatch[1]}, ${bboxMatch[2]}, ${bboxMatch[3]}, ${bboxMatch[4]}]`);
            } else {
                console.log(`${name}: NO BBOX FOUND`);
            }
        });
    });
});
