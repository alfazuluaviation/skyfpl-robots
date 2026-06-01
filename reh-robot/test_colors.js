const fs = require('fs');
const { PNG } = require('pngjs');

const data = fs.readFileSync('vitoria_tile.png');
const png = PNG.sync.read(data);

let solidColorCount = {};
let alpha0 = 0;
let totalPixels = png.width * png.height;
let whitePixels = 0;

for (let y = 0; y < png.height; y++) {
    for (let x = 0; x < png.width; x++) {
        let idx = (png.width * y + x) << 2;
        let r = png.data[idx];
        let g = png.data[idx + 1];
        let b = png.data[idx + 2];
        let a = png.data[idx + 3];

        if (a === 0) {
            alpha0++;
            // When converting transparent to RGB, PIL usually puts 0,0,0
            r = 0; g = 0; b = 0;
        }

        let key = `${r},${g},${b}`;
        if (!solidColorCount[key]) solidColorCount[key] = 0;
        solidColorCount[key]++;

        if (r === 255 && g === 255 && b === 255) {
            whitePixels++;
        }
    }
}

console.log('Total pixels:', totalPixels);
console.log('Transparent pixels:', alpha0);
console.log('White pixels:', whitePixels);
console.log('Unique colors:', Object.keys(solidColorCount).length);

const colors = Object.keys(solidColorCount);
if (colors.length === 1) {
    console.log('100% solid color:', colors[0]);
}

if (whitePixels / totalPixels >= 0.93) {
    console.log('White pixels > 93%');
}
