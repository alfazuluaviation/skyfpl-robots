function latLngToTile(lat, lng, zoom) {
    const n = Math.pow(2.0, zoom);
    const x = Math.floor((lng + 180.0) / 360.0 * n);
    const lat_rad = lat * Math.PI / 180.0;
    const y = Math.floor((1.0 - Math.log(Math.tan(lat_rad) + (1 / Math.cos(lat_rad))) / Math.PI) / 2.0 * n);
    return [x, y];
}

const REH_BBOXES = {
    "REH_WJ2_RIO_DE_JANEIRO": [-44.813333333333325, -24.00166666666666, -41.76017583793333, -21.81760169266666],
    "REH_XP_SAO_PAULO": [-47.89661794556251, -24.503348570316604, -44.395672115362515, -22.285199031516605],
    "REH_XR_VITORIA": [-40.66666666666665, -20.583333333333336, -39.91648482946665, -19.799779604533324]
};

for (const [code, bbox] of Object.entries(REH_BBOXES)) {
    console.log(`\n=== ${code} ===`);
    let total = 0;
    for (let z = 8; z <= 11; z++) {
        const [x_min, y_max_tile] = latLngToTile(bbox[1], bbox[0], z);
        const [x_max, y_min_tile] = latLngToTile(bbox[3], bbox[2], z);
        
        let x_start = Math.min(x_min, x_max);
        let x_end = Math.max(x_min, x_max);
        let y_start = Math.min(y_min_tile, y_max_tile);
        let y_end = Math.max(y_min_tile, y_max_tile);
        
        x_start = Math.max(0, x_start - 1);
        y_start = Math.max(0, y_start - 1);
        x_end += 1;
        y_end += 1;
        
        const count = (x_end - x_start + 1) * (y_end - y_start + 1);
        total += count;
        console.log(`Z=${z}: X:${x_start}..${x_end}, Y:${y_start}..${y_end} -> ${count} tiles`);
    }
    console.log(`Total: ${total}`);
}
